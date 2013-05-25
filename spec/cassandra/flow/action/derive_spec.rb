require 'spec_helper'

describe Cassandra::Flow::Action::Derive do
  let(:requests) { Mapper[:requests] }
  let(:backup) { Mapper[:requests_backup] }

  before do
    reset requests, backup
  end

  it 'should derive data from source to target' do
    Cassandra::Flow
      .new(requests)
      .target(backup)
      .derive {|it| it.merge(archive: true) }
      .setup!

    requests.insert project_id: 72, id: 14
    requests.insert project_id: 72, id: 16

    backup.all.should == [
      { project_id: '72', id: '14', archive: 'true' },
      { project_id: '72', id: '16', archive: 'true' }
    ]
  end
end
