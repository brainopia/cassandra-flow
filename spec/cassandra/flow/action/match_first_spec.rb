require 'spec_helper'

describe Cassandra::Flow::Action::MatchFirst do
  context 'match on other mapper' do
    before do
      Cassandra::Flow
        .source(facts)
        .match_first(facts2) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(views)
    end

    it 'should match first record of corresponding table' do
      facts.insert project_id: 72, id: 14
      views.all.should == [{ project_id: 72, id: 14, matched_id: 404 }]

      facts2.insert project_id: 72, id: 10
      views.all.should == [{ project_id: 72, id: 14, matched_id: 10 }]

      facts2.insert project_id: 72, id: 11
      views.all.should == [{ project_id: 72, id: 14, matched_id: 10 }]

      facts2.insert project_id: 72, id: 3
      views.all.should == [{ project_id: 72, id: 14, matched_id: 3 }]
    end

    it 'should support removal' do
      facts.insert project_id: 72, id: 14
      facts.remove project_id: 72, id: 14
      views.all.should be_empty

      facts2.insert project_id: 72, id: 10
      views.all.should be_empty
    end

    it 'should support removal of a match' do
      facts2.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 10
      views.all.should == [{ project_id: 72, id: 10, matched_id: 14 }]

      facts2.remove project_id: 72, id: 14
      views.all.should == [{ project_id: 72, id: 10, matched_id: 404 }]
    end
  end

  context 'match on itself' do
    before do
      Cassandra::Flow
        .source(facts)
        .match_first(facts) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(views)
    end

    it 'should support insertion' do
      facts.insert project_id: 72, id: 14
      views.all.should == [{ project_id: 72, id: 14, matched_id: 14 }]

      facts.insert project_id: 72, id: 10
      views.all.should == [
        { project_id: 72, id: 10, matched_id: 10 },
        { project_id: 72, id: 14, matched_id: 10 }
      ]

      facts.insert project_id: 72, id: 11
      views.all.should == [
        { project_id: 72, id: 10, matched_id: 10 },
        { project_id: 72, id: 11, matched_id: 10 },
        { project_id: 72, id: 14, matched_id: 10 }
      ]
    end

    it 'should support removal' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 10
      facts.insert project_id: 72, id: 11

      facts.remove project_id: 72, id: 14
      views.all.should == [
        { project_id: 72, id: 10, matched_id: 10 },
        { project_id: 72, id: 11, matched_id: 10 }
      ]

      facts.remove project_id: 72, id: 10
      views.all.should == [{ project_id: 72, id: 11, matched_id: 11 }]

      facts.remove project_id: 72, id: 11
      views.all.should be_empty
    end
  end
end
