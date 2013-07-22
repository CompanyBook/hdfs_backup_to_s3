require 'rspec'
require_relative '../lib/run_backup'

module Logging
  def self.create
    Logger.new(STDOUT)
  end
end

describe RunBackup do
  let(:back) { RunBackup.new }

  it 'should do run backup ' do
    back.start_backup('testdata', 'test-backup-hdfs_backup_to_s3')
  end
end