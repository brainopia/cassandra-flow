require 'spec_helper'

describe Cassandra::Flow::Action::Notify do
  let(:data) {{ project_id: 14, id: 12, data: 'content' }}
  let(:data2) {{ project_id: 14, id: 15, data: 'content2' }}

  it 'should notify all changes' do
    receiver = double :receiver
    receiver.should_receive(:get).with(data)

    Cassandra::Flow.source(facts).notify do |data|
      receiver.get data
    end

    facts.insert data
  end

  it 'should notify specific changes' do
    receiver = double :receiver
    receiver.should_receive(:get).with(data)
    receiver.should_not_receive(:get).with(data2)

    Cassandra::Flow.source(facts).notify(:remove) do |data|
      receiver.get data
    end

    facts.insert data2
    facts.remove data
  end
end
