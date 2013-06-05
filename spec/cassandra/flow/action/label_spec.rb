require 'spec_helper'

describe Cassandra::Flow::Action::Label do
  it 'should have a suffix' do
    Cassandra::Flow.source(facts).label('test').action.suffix.should == 'test'
  end
end
