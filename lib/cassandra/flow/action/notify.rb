class Cassandra::Flow::Action::Notify < Cassandra::Flow::Action
  auto_setup!
  attr_reader :target

  def initialize(type=nil, &callback)
    @type     = type
    @callback = callback
  end

  def propagate(type, data)
    if not @type or @type == type
      @callback.call data
    end
  end
end
