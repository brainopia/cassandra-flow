require 'spec_helper'

describe Cassandra::Flow::Action::Derive do
  it 'should derive data from source to target' do
    Cassandra::Flow
      .new(facts)
      .derive {|it| it.merge(archive: true) }
      .target(views)

    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 16

    views.all.should == [
      { project_id: '72', id: '14', archive: 'true' },
      { project_id: '72', id: '16', archive: 'true' }
    ]
  end
end
