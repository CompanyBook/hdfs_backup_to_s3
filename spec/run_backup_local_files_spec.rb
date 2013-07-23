require 'rspec'
require_relative '../lib/run_backup'

module Logging
  def self.create
    Logger.new(STDOUT)
  end
end

describe RunBackup do
  let(:back) { RunBackup.new('testdata', 'test-backup-hdfs_backup_to_s3') }

  it 'should do run backup ' do
    back.start_backup()
  end

  it 'should do get s3 files' do
    #files = back.get_s3_files('test-backup-hdfs_backup_to_s3', 'table-1')
    files = RunBackup.new().get_s3_files('BACKUP-FULL-TAKE-4-2013.06.10', 'web_crawl-0-1370732400000-FULL-1370732400000')
    puts '-------'
    puts files
  end

end