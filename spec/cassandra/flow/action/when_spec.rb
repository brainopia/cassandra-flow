require 'spec_helper'

describe Cassandra::Flow::Action::When do
  before do
    Cassandra::Flow
      .new(facts)
      .when(:id, filter)
      .target(views)
  end

  context 'simple filter' do
    let(:filter) { 16 }

    it 'should select data' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 16
      views.all.should == [{ project_id: 72, id: 16 }]
    end
  end

  context 'array filter' do
    let(:filter) { [16, 18] }

    it 'should select data' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 16
      facts.insert project_id: 72, id: 18

      views.all.should == [
        { project_id: 72, id: 16 },
        { project_id: 72, id: 18 }
      ]
    end
  end
end
