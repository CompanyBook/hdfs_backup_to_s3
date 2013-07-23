require_relative 'logging'

class RunBackup
  include Logging

  def start_backup(hdfs_path, s3_dir)
    mkdir('hdfs-to-s3')

    hdfs_tables = get_catalogs_with_size(hdfs_path)
    total_size = get_size_from_folders(hdfs_tables)
    report_status "processing root folder '#{hdfs_path}' with #{hdfs_tables.length} folders. Total size=#{total_size}"

    hdfs_tables.each do |table_path, size|
      table_name = get_last_name_from_path(table_path)
      report_status "processing:'#{table_path}' size: #{size}"

      process_sub_folders(s3_dir, table_name, table_path)
    end
  end

  def process_sub_folders(s3_dir, table_name, table_path)
    mkdir("hdfs-to-s3/#{table_name}")
    get_catalogs_with_size(table_path).each do |hdfs_file_path, file_size|
      file_name = get_last_name_from_path(hdfs_file_path)
      log.info "hdsf_file '#{hdfs_file_path}' size:#{file_size}"
      copy_local(hdfs_file_path, file_name, file_size, table_name) do |local_file, folder|
        transfer_to_s3(local_file, folder, s3_dir)
      end
    end
    rm_dir("hdfs-to-s3/#{table_name}")
  end

  def copy_local(hdfs_file_path, file_name, file_size, folder, &block)
    shell_cmd("hadoop fs -copyToLocal #{hdfs_file_path} hdfs-to-s3/#{folder}")
    block.call(file_name, folder)
    rm_file "hdfs-to-s3/#{folder}/#{file_name}"
  end

  def transfer_to_s3(file, folder, s3_dir)
    return 0 if file == '_logs'
    with_retry(50, "#{file} => #{s3_dir}") {
      ret_code, files = shell_cmd("s3cmd --no-encrypt put hdfs-to-s3/#{folder}/#{file} s3://companybook-backup/#{s3_dir}/#{folder}/#{file}")
      ret_code
    }
  end

  def get_last_name_from_path(sub)
    sub.split('/').last
  end

  def get_catalogs_with_size(path='')
    ret_code, files = shell_cmd("hadoop fs -du #{path}")
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
    log.info "cmd:'#{cmd}' exit:#{exitstatus} time:#{'%.1f' % (Time.now-start)}"
    return exitstatus, result
  end

  def with_retry(retry_cnt, msg, &code)
    (1..retry_cnt).each do |i|
      r_code = code.call()
      return if r_code == 0
      sleep_time = 10*i
      log.warn msg
      log.warn "retry:#{i} sleeping:#{sleep_time}"
      sleep sleep_time
    end
    raise "retry #{retry_cnt} failed: #{msg}"
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