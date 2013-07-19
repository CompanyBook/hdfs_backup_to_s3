class RunBackup
  def start_backup(hdfs_path, s3_dir)
    hdfs_tables = get_catalogs_with_size(hdfs_path)

    total_size = get_size_from_folders(hdfs_tables)
    puts "size #{hdfs_path} in #{hdfs_tables.length} sub-folders is #{total_size}"

    hdfs_tables.each do |sub, size|
      table_name = get_last_name_from_path(sub)
      puts "root - #{sub} - #{size}"
      puts "table_name - #{table_name}"

      get_catalogs_with_size(sub).each do |hdfs_file_path, s|
        file_name = get_last_name_from_path(hdfs_file_path)
        puts " sub  - #{hdfs_file_path} - #{s}"
        puts " file_name - #{file_name}"
        copy_local(hdfs_file_path, file_name, table_name) do |local_file, folder|
          transfer_to_s3(local_file, folder, s3_dir)
        end
      end
    end
  end

  def copy_local(hdfs_file_path, file_name, folder, &block)
    shell_cmd("hadoop fs -copyToLocal #{hdfs_file_path} => #{folder}")
    block.call(file_name, folder)
    shell_cmd"rm -f #{file_name}"
  end

  def transfer_to_s3(file, folder, s3_dir)
    shell_cmd("s3cmd --no-encrypt put #{folder}/#{file} s3://companybook-backup/#{s3_dir}/#{folder}/#{file}")
  end

  def get_last_name_from_path(sub)
    sub.split('/').last
  end

  def get_catalogs_with_size(path='')
    puts "getting sub folders of #{path}"
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
    result = %x[#{cmd}]
    result = result.split("\n") if result
    return $?.exitstatus, result
  end
end