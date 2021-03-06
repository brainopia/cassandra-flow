class Cassandra::Flow::Action::Check < Cassandra::Flow::Action
  action!

  def setup!(type=nil, &callback)
    @type     = type
    @callback = callback
  end

  def propagate(type, data)
    if not @type or @type == type
      @callback.call data
    end
    data
  end
end
