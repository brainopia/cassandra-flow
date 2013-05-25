class Cassandra::Flow::Action::Derive < Cassandra::Flow::Action
  def initialize(&block)
    @callback = block
  end

  def propagate(type, data)
    @callback.call data
  end
end
