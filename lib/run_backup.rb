require_relative 'logging'

class RunBackup
  include Logging

  def start_backup(hdfs_path, s3_dir)
    mkdir('hdfs-to-s3')

    hdfs_tables = get_catalogs_with_size(hdfs_path)

    total_size = get_size_from_folders(hdfs_tables)
    msg = "size #{hdfs_path} in #{hdfs_tables.length} sub-folders is #{total_size}"
    log.info msg

    hdfs_tables.each do |sub, size|
      table_name = get_last_name_from_path(sub)
      puts "root - #{sub} - #{size}"
      log.info "root - #{sub} - #{size}"
      log.info "table_name - #{table_name}"
      mkdir("hdfs-to-s3/#{table_name}")

      get_catalogs_with_size(sub).each do |hdfs_file_path, s|
        file_name = get_last_name_from_path(hdfs_file_path)
        log.info " sub  - #{hdfs_file_path} - #{s}"
        log.info " file_name - #{file_name}"
        copy_local(hdfs_file_path, file_name, table_name) do |local_file, folder|
          transfer_to_s3(local_file, folder, s3_dir)
        end
      end
      shell_cmd "rm -rf hdfs-to-s3/#{table_name}"
    end
  end

  def mkdir(dir)
    shell_cmd("mkdir -p #{dir}")
  end

  def copy_local(hdfs_file_path, file_name, folder, &block)
    shell_cmd("hadoop fs -copyToLocal #{hdfs_file_path} hdfs-to-s3/#{folder}")
    block.call(file_name, folder)
    shell_cmd "rm -f hdfs-to-s3/#{folder}/#{file_name}"
  end

  def transfer_to_s3(file, folder, s3_dir)
    with_retry(50, "#{file} => #{s3_dir}") {
      ret_code, files = shell_cmd("s3cmd --no-encrypt put hdfs-to-s3/#{folder}/#{file} s3://companybook-backup/#{s3_dir}/#{folder}/#{file}")
      ret_code
    }
  end

  def get_last_name_from_path(sub)
    sub.split('/').last
  end

  def get_catalogs_with_size(path='')
    puts "getting sub folders of #{path}"
    log.info "getting sub folders of #{path}"
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
    log.info "cmd = '#{cmd}'"
    result = %x[#{cmd}]
    result = result.split("\n") if result
    exitstatus = $?.exitstatus
    log.info "cmd = '#{cmd}' exit:#{exitstatus}"
    return exitstatus, result
  end

  def with_retry(retry_cnt, msg, &code)
    (1..retry_cnt).each do |i|
      r_code = code.call()
      log.info "return code= #{r_code}"
      return if r_code == 0
      sleep_time = 10*i
      log.warn msg
      log.warn "retry:#{i} sleeping:#{sleep_time}"
      sleep sleep_time
    end
    raise "retry #{retry_cnt} failed: #{msg}"
  end

end