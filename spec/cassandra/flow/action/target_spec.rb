require 'spec_helper'

describe Cassandra::Flow::Action::Target do
  let(:flow) do
    Cassandra::Flow.source(facts).target(views)
  end

  before { flow }

  it 'should flow data from source to target' do
    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 16

    views.all.should have(2).items
    views.all.should == facts.all
  end

  it 'return a flow with new source' do
    flow.source.should == views
  end
end
