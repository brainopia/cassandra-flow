require 'spec_helper'

describe Cassandra::Flow::Action::MatchTime do
  let(:base_time) { Time.at Time.now.to_i + 0.001002 }
  let(:round_time) { Time.at Time.now.to_i + 0.001 }

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
      event_map.all.should == [{ project_id: 72, matched_id: 404, time: round_time }]

      events2.insert project_id: 72, id: 14, time: base_time - 1
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time + 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time - 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: round_time }]
    end

    it 'should support equal time matching on insert' do
      events2.insert project_id: 72, id: 10, time: base_time
      events.insert project_id: 72, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: round_time }]
    end

    it 'should support removal' do
      events.insert project_id: 72, time: base_time
      events.remove project_id: 72, time: base_time
      event_map.all.should be_empty

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should be_empty
    end

    it 'should support removal of a match' do
      events2.insert project_id: 72, id: 14, time: base_time
      events.insert project_id: 72, time: base_time
      events.insert project_id: 72, time: base_time + 100_000
      event_map.all.should == [
        { project_id: 72, matched_id: 14, time: round_time },
        { project_id: 72, matched_id: 14, time: round_time + 100_000 }
      ]

      events2.remove project_id: 72, id: 14, time: base_time
      event_map.all.should == [
        { project_id: 72, matched_id: 404, time: round_time },
        { project_id: 72, matched_id: 404, time: round_time + 100_000 }
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
        }.match_time(events2, source: :diff) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(diff_event_map)
    end

    it 'should match first record of corresponding table' do
      events.insert project_id: 72, time: base_time
      diff_event_map.all.should == [{ project_id: 72, matched_id: 404, diff: round_time + diff }]

      events2.insert project_id: 72, id: 14, time: base_time + diff + 1
      diff_event_map.all.should == [{ project_id: 72, matched_id: 404, diff: round_time + diff }]

      events2.insert project_id: 72, id: 10, time: base_time + diff
      diff_event_map.all.should == [{ project_id: 72, matched_id: 10, diff: round_time + diff }]
    end
  end

  context 'match after' do
    before do
      Cassandra::Flow
        .source(events)
        .match_time(events2, after: true) {|data, match|
          data.merge matched_id: match ? match[:id] : 404
        }.target(event_map)
    end

    it 'should match first record of corresponding table' do
      events.insert project_id: 72, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 404, time: round_time }]

      events2.insert project_id: 72, id: 14, time: base_time + 1
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time - 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time + 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: round_time }]
    end

    it 'should support equal time matching on insert' do
      events2.insert project_id: 72, id: 10, time: base_time
      events.insert project_id: 72, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: round_time }]
    end

    it 'should support removal' do
      events.insert project_id: 72, time: base_time
      events.remove project_id: 72, time: base_time
      event_map.all.should be_empty

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should be_empty
    end

    it 'should support removal of a match' do
      events2.insert project_id: 72, id: 14, time: base_time
      events.insert project_id: 72, time: base_time
      events.insert project_id: 72, time: base_time - 100_000
      event_map.all.should == [
        { project_id: 72, matched_id: 14, time: round_time - 100_000 },
        { project_id: 72, matched_id: 14, time: round_time }
      ]

      events2.remove project_id: 72, id: 14, time: base_time
      event_map.all.should == [
        { project_id: 72, matched_id: 404, time: round_time - 100_000 },
        { project_id: 72, matched_id: 404, time: round_time }
      ]
    end
  end

  context 'automatic field match in interval' do
    before do
      Cassandra::Flow
        .source(events)
        .match_time(events2, interval: 300) {|data, matches|
          match = matches.first
          data.merge matched_id: match ? match[:id] : 404
        }.target(event_map)
    end

    it 'should update data from interval' do
      events.insert project_id: 72, time: base_time
      event_map.all.should == [{ project_id: 72, matched_id: 404, time: round_time }]

      events2.insert project_id: 72, id: 14, time: base_time - 1
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time + 4
      event_map.all.should == [{ project_id: 72, matched_id: 14, time: round_time }]

      events2.insert project_id: 72, id: 10, time: base_time - 4
      event_map.all.should == [{ project_id: 72, matched_id: 10, time: round_time }]

      events2.insert project_id: 72, id: 9, time: base_time - 300
      event_map.all.should == [{ project_id: 72, matched_id: 9, time: round_time }]

      events2.insert project_id: 72, id: 8, time: base_time - 301
      event_map.all.should == [{ project_id: 72, matched_id: 9, time: round_time }]

      events2.insert project_id: 72, id: 8, time: base_time - 299
      event_map.all.should == [{ project_id: 72, matched_id: 9, time: round_time }]
    end

    it 'should support removal' do
      events.insert project_id: 72, time: base_time
      events.remove project_id: 72, time: base_time
      event_map.all.should be_empty

      events2.insert project_id: 72, id: 10, time: base_time
      event_map.all.should be_empty
    end

    it 'should support removal of a match' do
      events2.insert project_id: 72, id: 14, time: base_time - 300
      events.insert project_id: 72, time: base_time - 300
      events.insert project_id: 72, time: base_time
      events.insert project_id: 72, time: base_time + 100_000
      event_map.all.should == [
        { project_id: 72, matched_id: 14, time: round_time - 300 },
        { project_id: 72, matched_id: 14, time: round_time },
        { project_id: 72, matched_id: 404, time: round_time + 100_000 }
      ]

      events2.remove project_id: 72, id: 14, time: base_time - 300
      event_map.all.should == [
        { project_id: 72, matched_id: 404, time: round_time - 300 },
        { project_id: 72, matched_id: 404, time: round_time },
        { project_id: 72, matched_id: 404, time: round_time + 100_000 }
      ]
    end
  end
end
