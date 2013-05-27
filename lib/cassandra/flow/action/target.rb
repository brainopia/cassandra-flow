class Cassandra::Flow::Action::Target < Cassandra::Flow::Action
  def initialize(flow, mapper=nil)
    @target = mapper
    flow.setup!
  end

  def propagate(type, data)
    @target.send type, data
  end
end
