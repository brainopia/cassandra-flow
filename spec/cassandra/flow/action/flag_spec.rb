require 'spec_helper'

describe Cassandra::Flow::Action::Flag do
  it 'should flag data' do
    Cassandra::Flow
      .new(facts)
      .flag(:min, :project_id) {|new, old|
        new[:id] < old[:id]
      }.target(views)

    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 16
    facts.insert project_id: 72, id: 18

    facts.insert project_id: 14, id: 18
    facts.insert project_id: 14, id: 16
    facts.insert project_id: 14, id: 14

    views.all.select {|it| it[:min] }.should == [
      { project_id: 72, id: 14, min: true },
      { project_id: 14, id: 14, min: true }
    ]
  end
end
