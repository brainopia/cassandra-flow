require 'spec_helper'

describe Cassandra::Flow do
  it 'should propagate all changes' do
    Cassandra::Flow
      .source(facts)
      .if_match(:project_id, 14)
      .derive {|it|
        it[:id] += 1
        it[:archive] = true
        it
      }.target(views)

    facts.insert project_id: 14, id: 1
    facts.insert project_id: 15, id: 1

    views.all.should == [{ project_id: 14, id: 2, archive: true }]

    facts.remove project_id: 14, id: 1
    views.all.should be_empty
  end
end
