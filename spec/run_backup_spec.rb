require 'rspec'
require_relative '../lib/run_backup'

class RunBackup
  def shell_cmd(cmd)
    puts "cmd:'#{cmd}"
    back_up_root_result = [
        '16000          /user/xxx/BACKUP/table-1',
        '11000          /user/xxx/BACKUP/table-2',
    ]

    back_up_sub_folder_result_1 = [
        '1500          /user/xxx/BACKUP/table-1/part-m-000001',
        '1000          /user/xxx/BACKUP/table-1/part-m-000002',
    ]
    back_up_sub_folder_result_2 = [
        '1500          /user/xxx/BACKUP/table-2/part-m-000001',
        '1000          /user/xxx/BACKUP/table-2/part-m-000002',
    ]

    return [1, back_up_root_result] if cmd.end_with? '/user/xxx/BACKUP'
    return [1, back_up_sub_folder_result_1] if cmd.end_with? '/user/xxx/BACKUP/table-1'
    return [1, back_up_sub_folder_result_2] if cmd.end_with? '/user/xxx/BACKUP/table-2'
    [1, ['error']]
  end
end

describe RunBackup do
  let(:back) { RunBackup.new }

  it 'should some get folder and size' do
    back.stub(:shell_cmd).and_return([0, ['20    /path1', '10    /path2']])
    p back.get_catalogs_with_size

    back.get_catalogs_with_size.should == [["/path2", "10"], ["/path1", "20"]]
  end

  it 'should some get total size' do
    p back.get_size_from_folders([["/path1", "10"], ["/path2", "20"]])
  end

  it 'should do run backup ' do
    back.start_backup('/user/xxx/BACKUP', '2013.06.20')
  end
end