require 'spec_helper'

describe Cassandra::Flow::Action::When do
  let(:requests) { Mapper[:requests] }
  let(:backup) { Mapper[:requests_backup] }

  before do
    reset requests, backup
  end

  it 'should filter data from source to target' do
    Cassandra::Flow
      .new(requests)
      .target(backup)
      .when(:id, '16')
      .setup!

    requests.insert project_id: 72, id: 14
    requests.insert project_id: 72, id: 16

    backup.all.should == [{ project_id: '72', id: '16' }]
  end
end
