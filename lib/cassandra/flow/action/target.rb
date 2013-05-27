class Cassandra::Flow::Action::Target < Cassandra::Flow::Action
  auto_setup!
  attr_reader :target

  def initialize(mapper=nil)
    @target = mapper
  end

  def propagate(type, data)
    target.send type, data
  end
end
