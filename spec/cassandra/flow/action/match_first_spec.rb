require 'spec_helper'

describe Cassandra::Flow::Action::MatchFirst do
  it 'should match first record of corresponding table' do
    Cassandra::Flow
      .new(facts)
      .match_first(facts2) {|data, match|
        data[:matched_id] = match ? match[:id] : 404
        data
      }.target(views)

    facts.insert project_id: 72, id: 14
    views.all.should == [{ project_id: 72, id: 14, matched_id: 404 }]

    facts2.insert project_id: 72, id: 10
    views.all.should == [{ project_id: 72, id: 14, matched_id: 10 }]

    facts2.insert project_id: 72, id: 11
    views.all.should == [{ project_id: 72, id: 14, matched_id: 10 }]

    facts2.insert project_id: 72, id: 3
    views.all.should == [{ project_id: 72, id: 14, matched_id: 3 }]
  end
end
