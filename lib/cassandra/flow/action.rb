class Cassandra::Flow::Action
  def self.inherited(klass)
    Cassandra::Flow.action klass
  end

  def initialize(flow)
    @flow = flow
  end

  def setup!
  end
end
