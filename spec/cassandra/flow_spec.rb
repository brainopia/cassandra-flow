require 'spec_helper'

describe Cassandra::Flow do
  it 'should flow data from source to target' do
    Cassandra::Flow
      .new(facts)
      .target(views)
      .setup!

    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 16

    views.all.should have(2).items
    views.all.should == facts.all
  end
end
