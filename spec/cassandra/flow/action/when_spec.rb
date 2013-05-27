require 'spec_helper'

describe Cassandra::Flow::Action::When do
  it 'should filter data from source to target' do
    Cassandra::Flow
      .new(facts)
      .when(:id, 16)
      .target(views)

    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 16

    views.all.should == [{ project_id: 72, id: 16 }]
  end
end
