require "rspec/core/rake_task"
require_relative 'lib/run_backup'

desc "Run RSpec unit tests"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern       = 'spec/*_spec.rb'
end

desc "back hdfs to s3"
task :backup, [:hdfs_path, :s3_dir, :report_only] do |t, args|
  args.with_defaults(
    :hdfs_path => '/user/ben/BACKUP',
    :s3_dir => 'BACKUP-FULL-TAKE-4-2013.06.10',
    :report_only => true
  )
  p args
  report_only = args[:report_only].downcase == 'true' ? true : false
  RunBackup.new(args[:hdfs_path], args[:s3_dir], report_only).start_backup()
end


