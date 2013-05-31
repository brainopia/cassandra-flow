require 'spec_helper'

describe Cassandra::Flow::Action::Aggregate do
  before do
    Cassandra::Flow
      .source(facts)
      .aggregate(:id) {|data, previous|
        previous ||= {}
        data[:total] = previous[:total].to_i + 1
        data
      }.target(views)
  end

  it 'should aggregate data' do
    facts.insert project_id: 72, id: 14
    views.all.should == [{ project_id: 72, id: 14, total: 1 }]

    facts.insert project_id: 72, id: 14
    views.all.should == [{ project_id: 72, id: 14, total: 2 }]

    facts.remove project_id: 72, id: 14
    views.all.should == [{ project_id: 72, id: 14, total: 1 }]

    facts.remove project_id: 72, id: 14
    views.all.should == []

    facts.remove project_id: 72, id: 14
    views.all.should == []
  end
end
