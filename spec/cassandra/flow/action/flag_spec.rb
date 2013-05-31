require 'spec_helper'

describe Cassandra::Flow::Action::Flag do
  before do
    Cassandra::Flow
      .source(facts)
      .flag(:min, :project_id) {|new, old|
        new[:id] < old[:id]
      }.target(views)
  end

  it 'should insert with flag data' do
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

  it 'should remove with flag data' do
    facts.insert project_id: 72, id: 14
    views.all.should == [{ project_id: 72, id: 14, min: true }]

    facts.remove project_id: 72, id: 14
    views.all.should == []
  end

  it 'should keep flag after removal' do
    facts.insert project_id: 72, id: 14
    facts.insert project_id: 72, id: 18
    facts.insert project_id: 72, id: 10
    facts.insert project_id: 72, id: 15

    views.all.find {|it| it[:min] }.should include(id: 10)
    facts.remove project_id: 72, id: 14
    views.all.find {|it| it[:min] }.should include(id: 10)
    facts.remove project_id: 72, id: 10
    views.all.find {|it| it[:min] }.should include(id: 15)
    facts.remove project_id: 72, id: 15
    views.all.find {|it| it[:min] }.should include(id: 18)
    facts.remove project_id: 72, id: 18
    views.all.should == []
  end
end
