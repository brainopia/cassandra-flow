require 'spec_helper'

describe Cassandra::Flow::Action::MatchFirst do
  before do
    Cassandra::Flow
      .new(events)
      .match_time(events2) {|data, match|
        data[:matched_id] = match ? match[:id] : 404
        data
      }.target(event_map)
  end

  let(:base_time) { Time.at Time.now.to_i }

  it 'should match first record of corresponding table' do
    events.insert project_id: 72, time: base_time
    event_map.all.should == [{ project_id: 72, matched_id: 404, time: base_time }]

    events2.insert project_id: 72, id: 14, time: base_time - 1
    event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

    facts2.insert project_id: 72, id: 10, time: base_time + 4
    event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

    facts2.insert project_id: 72, id: 10, time: base_time - 4
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
    events2.insert project_id: 72, id: 14, time: base_time - 100_000
    events.insert project_id: 72, time: base_time
    event_map.all.should == [{ project_id: 72, matched_id: 14, time: base_time }]

    events2.remove project_id: 72, id: 14, time: base_time - 100_000
    event_map.all.should == [{ project_id: 72, matched_id: 404, time: base_time }]
  end
end
