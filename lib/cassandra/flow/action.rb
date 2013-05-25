class Cassandra::Flow::Action
  def self.inherited(klass)
    Cassandra::Flow.action klass
  end

  def setup!(flow)
  end
end
