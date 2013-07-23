require_relative 'logging'

class RunBackup
  include Logging

  def initialize(hdfs_path, s3_dir, report_only=false)
    @hdfs_path = hdfs_path
    @s3_dir = s3_dir
    @report_only = report_only
  end

  def start_backup()
    mkdir('hdfs-to-s3')

    hdfs_tables = get_catalogs_with_size(@hdfs_path)
    total_size = get_size_from_folders(hdfs_tables)
    report_status "processing root folder '#{@hdfs_path}' with #{hdfs_tables.length} folders. Total size=#{total_size}"

    create_report(hdfs_tables)

    process(hdfs_tables) unless @report_only
  end

  def create_report(hdfs_tables)
    process(hdfs_tables, true)
  end

  def process(hdfs_tables, report_only=false)
    @report = Hash.new { |hash, key| hash[key] = [] }
    hdfs_tables.each do |table_path, size|
      table_name = get_last_name_from_path(table_path)
      report_status "processing:'#{table_path}' size: #{to_gbyte size}"

      process_sub_folders(table_name, table_path, report_only)
    end
    format_report
  end

  def process_sub_folders(table_name, table_path, report_only=false)
    mkdir("hdfs-to-s3/#{table_name}")

    s3_files = get_s3_files(table_name)

    get_catalogs_with_size(table_path).each do |hdfs_file_path, hdfs_file_size|
      file_name = get_last_name_from_path(hdfs_file_path)
      log.info "hdsf_file '#{hdfs_file_path}' size:#{hdfs_file_size}"

      size_from_s3_file = s3_files[file_name]
      unless file_exist_in_s3?(size_from_s3_file, file_name, hdfs_file_size+'1', table_name)
        process_file(file_name, hdfs_file_path, table_name) unless report_only
      end
      @report[table_name] << [file_name, hdfs_file_size, size_from_s3_file]
    end
    rm_dir("hdfs-to-s3/#{table_name}")
  end

  def format_report()
    report_status "---- #{@s3_dir} ----"
    hdfs_size = 0
    s3_size = 0
    diff_size = 0
    @report.each do |table, files|
      size_of_all_hdfs_files = files.map { |file_name, hdsf_file_size, s3_file_size| hdsf_file_size.to_i }.inject(0, :+)
      hdfs_size += size_of_all_hdfs_files
      size_of_all_s3_files = files.map { |file_name, hdsf_file_size, s3_file_size| s3_file_size.to_i }.inject(0, :+)
      s3_size += size_of_all_s3_files

      diff_files = files.find_all { |file_name, hdsf_file_size, s3_file_size| hdsf_file_size!=s3_file_size }
      size_of_diff_files = diff_files.map { |file_name, hdsf_file_size, s3_file_size| hdsf_file_size.to_i }.inject(0, :+)
      diff_size += size_of_diff_files

      report_status "#{table} all cnt:#{files.length} hdfs_size:#{to_gbyte size_of_all_hdfs_files} s3_size:#{to_gbyte size_of_all_s3_files}"
      report_status "#{table} diff cnt:#{diff_files.length} hdfs_size:#{to_gbyte size_of_diff_files}"
      report_status "#{diff_files.map { |file_name, hdsf_file_size, s3_file_size| file_name }}"
    end
    report_status "hdfs_size     : #{to_gbyte hdfs_size}"
    report_status "s3_size       : #{to_gbyte s3_size}"
    report_status "missing in s3 : #{to_gbyte diff_size}"
    report_status '----'
  end

  def to_gbyte(num)
    n = num.to_f
    return "#{num} bytes" if n < 10**3
    return '%.2f kb' % (n / 10**3) if n < 10**6
    return '%.2f Mb' % (n / 10**6) if n < 10**9
    return '%.2f Gb' % (n / 10**9) if n < 10**12
    '%.2f Tb' % (n / 10**12)
  end

  def file_exist_in_s3?(size_from_s3_file, file_name, file_size, table_name)
    unless size_from_s3_file
      log.info "#{file_name} not found in #{table_name} at s3:#{@s3_dir}/#{table_name}"
      return false
    end

    if file_size != size_from_s3_file
      log.warn "hdfs_file_size(#{file_size}) != size_from_s3_file(#{size_from_s3_file}) for #{table_name}/#{file_name}"
      return false
    end
    true
  end

  def get_s3_files(table_name)
    result = shell_cmd("s3cmd ls s3://companybook-backup/#{@s3_dir}/#{table_name}/")
    map = {}
    result.map { |line| line.split(/\s+/)[2..3] }.each do |size, path|
      file_name = get_last_name_from_path(path)
      map[file_name] = size
    end
    map
  end

  def process_file(file_name, hdfs_file_path, table_name)
    shell_cmd("hadoop fs -copyToLocal #{hdfs_file_path} hdfs-to-s3/#{table_name}")
    transfer_to_s3(file_name, table_name)
    rm_file "hdfs-to-s3/#{table_name}/#{file_name}"
  end

  def transfer_to_s3(file_name, table_name)
    return 0 if file_name == '_logs'
    with_retry(50, "#{file_name} => #{@s3_dir}") {
      shell_cmd("s3cmd --no-encrypt put hdfs-to-s3/#{table_name}/#{file_name} s3://companybook-backup/#{@s3_dir}/#{table_name}/#{file_name}")
    }
  end

  def get_last_name_from_path(sub)
    sub.split('/').last
  end

  def get_catalogs_with_size(path='')
    files = shell_cmd("hadoop fs -du #{path}")
    files.map { |line| line.split(/\s+/).reverse }.sort_by { |folder, size| size }.find_all { |folder, size| !folder.end_with? '_logs' }
  end

  def get_size_from_folders(folders='')
    folders.map { |folder, size| size.to_i }.inject(0, :+)
  end

  def get_files_with_size(path)
    shell_cmd("hadoop fs -du #{path}")
  end

  def shell_cmd(cmd)
    start = Time.now
    result = %x[#{cmd}]
    result = result.split("\n") if result
    exitstatus = $?.exitstatus
    log.info "cmd:'#{cmd}' time:#{'%.1f' % (Time.now-start)}"
    raise "exitstatus = #{exitstatus} for #{cmd}" if exitstatus != 0
    result
  end

  def with_retry(retry_cnt, msg, &code)
    exception = nil
    (1..retry_cnt).each do |i|
      begin
        return code.call()
      rescue => ex
        exception = ex
        sleep_time = 10*i
        logger.warn "#{ex.class}:#{ex.message}\n#{msg}\n#{ex.backtrace.join("\n")}"
        logger.warn "retry:#{i} sleeping:#{sleep_time}"
        sleep sleep_time
      end
    end
    raise exception
  end

  def mkdir(dir)
    shell_cmd("mkdir -p #{dir}")
  end

  def rm_dir(path)
    shell_cmd "rm -rf #{path}"
  end

  def rm_file(path)
    shell_cmd "rm -rf #{path}"
  end

  def report_status(msg)
    puts msg
    log.info msg
  end
end