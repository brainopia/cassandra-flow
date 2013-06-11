require 'spec_helper'

describe Cassandra::Flow::Action::MatchTime do
  let(:base_time) { Time.at Time.now.to_i }

  context 'automatic field match' do
    before do
      Cassandra::Flow
        .source(events)
        .match_time(events2) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(event_map)
    end

    it 'should match first record of corresponding table' do
      events.insert project_id: 72, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 404, time: base_time }]

      events2.insert project_id: 72, id: 14, time: base_time - 1
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

      events2.insert project_id: 72, id: 10, time: base_time + 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

      events2.insert project_id: 72, id: 10, time: base_time - 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: base_time }]
    end

    it 'should support removal' do
      events.insert project_id: 72, time: base_time
      events.remove project_id: 72, time: base_time
      event_map.all.should be_empty

      events2.insert project_id: 72, id: 10, time: base_time
      views.all.should be_empty
    end

    it 'should support removal of a match' do
      events2.insert project_id: 72, id: 14, time: base_time
      events.insert project_id: 72, time: base_time
      events.insert project_id: 72, time: base_time + 100_000
      event_map.all.should == [
        { project_id: 72, matched_id: 14, time: base_time },
        { project_id: 72, matched_id: 14, time: base_time + 100_000 }
      ]

      events2.remove project_id: 72, id: 14, time: base_time
      event_map.all.should == [
        { project_id: 72, matched_id: 404, time: base_time },
        { project_id: 72, matched_id: 404, time: base_time + 100_000 }
      ]
    end
  end

  context 'manual field match' do
    let(:diff) { 10 }

    before do
      Cassandra::Flow
        .source(events)
        .derive {|data|
          data = data.dup
          data.merge! diff: data.delete(:time) + diff
        }.match_time(events2, :diff) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(diff_event_map)
    end

    it 'should match first record of corresponding table' do
      events.insert project_id: 72, time: base_time
      diff_event_map.all.should == [{ project_id: 72, matched_id: 404, diff: base_time + diff }]

      events2.insert project_id: 72, id: 14, time: base_time + diff + 1
      diff_event_map.all.should == [{ project_id: 72, matched_id: 404, diff: base_time + diff }]

      events2.insert project_id: 72, id: 10, time: base_time + diff
      diff_event_map.all.should == [{ project_id: 72, matched_id: 10, diff: base_time + diff }]
    end
  end
end
