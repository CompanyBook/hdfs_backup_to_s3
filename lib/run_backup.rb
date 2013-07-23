require_relative 'logging'

class RunBackup
  include Logging

  def initialize(hdfs_path, s3_dir)
    @hdfs_path = hdfs_path
    @s3_dir = s3_dir
  end

  def start_backup()
    mkdir('hdfs-to-s3')

    hdfs_tables = get_catalogs_with_size(@hdfs_path)
    total_size = get_size_from_folders(hdfs_tables)
    report_status "processing root folder '#{@hdfs_path}' with #{hdfs_tables.length} folders. Total size=#{total_size}"

    hdfs_tables.each do |table_path, size|
      table_name = get_last_name_from_path(table_path)
      report_status "processing:'#{table_path}' size: #{size}"

      process_sub_folders(table_name, table_path)
    end
  end

  def process_sub_folders(table_name, table_path)
    mkdir("hdfs-to-s3/#{table_name}")

    s3_files = get_s3_files(table_name)

    get_catalogs_with_size(table_path).each do |hdfs_file_path, file_size|
      file_name = get_last_name_from_path(hdfs_file_path)
      log.info "hdsf_file '#{hdfs_file_path}' size:#{file_size}"

      unless file_exist_in_s3?(s3_files, file_name, file_size, table_name)
        process_file(file_name, hdfs_file_path, table_name)
      end
    end
    rm_dir("hdfs-to-s3/#{table_name}")
  end

  def file_exist_in_s3?(s3_files, file_name, file_size, table_name)
    size_from_s3_file = s3_files[file_name]

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
    files.map { |line| line.split(/\s+/).reverse }.sort_by { |folder, size| size }
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
    shell_cmd "rm -f #{path}"
  end

  def report_status(msg)
    puts msg
    log.info msg
  end
end