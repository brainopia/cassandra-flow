require 'spec_helper'

describe Cassandra::Flow do
  let(:requests) { Mapper[:requests] }
  let(:backup) { Mapper[:requests_backup] }

  before do
    reset requests, backup
  end

  it 'should flow data from source to target' do
    Cassandra::Flow
      .new(requests)
      .target(backup)
      .setup!

    requests.insert project_id: 72, id: 14
    requests.insert project_id: 72, id: 16

    backup.all.should have(2).items
    backup.all.should == requests.all
  end
end
