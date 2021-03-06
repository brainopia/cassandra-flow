require 'spec_helper'

describe Cassandra::Flow::Action::UnlessMatch do
  before do
    Cassandra::Flow
      .source(facts)
      .unless_match(field, filter)
      .target(views)
  end

  context 'simple filter' do
    let(:field)  { :id }
    let(:filter) { 16 }

    it 'should select data' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 16
      views.all.should == [{ project_id: 72, id: 14 }]
    end
  end

  context 'array filter' do
    let(:field)  { :id }
    let(:filter) { [16, 18] }

    it 'should select data' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 16
      facts.insert project_id: 72, id: 18

      views.all.should == [{ project_id: 72, id: 14 }]
    end
  end

  context 'present filter' do
    let(:field)  { :data }
    let(:filter) { described_class::PRESENT_SYMBOL }

    it 'should select data' do
      facts.insert project_id: 72, id: 14
      facts.insert project_id: 72, id: 10
      facts.insert project_id: 72, id: 18, data: 'content'

      views.all.should == [
        { project_id: 72, id: 10 },
        { project_id: 72, id: 14 }
      ]
    end
  end
end
