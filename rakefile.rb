require "rspec/core/rake_task"
require_relative 'lib/run_backup'

desc "Run RSpec unit tests"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern       = 'spec/*_spec.rb'
end

desc "back hdfs to s3"
task :backup, [:hdfs_path, :s3_dir] do |t, args|
  args.with_defaults(
    :hdfs_path => '/user/ben/BACKUP',
    :s3_dir => 'BACKUP-FULL-TAKE-4-2013.06.10',
  )
  p args
  RunBackup.new.start_backup(args[:hdfs_path], args[:s3_dir])
end


