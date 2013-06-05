class Cassandra::Flow::Action::Derive < Cassandra::Flow::Action
  action!

  def setup!(&callback)
    @callback = callback
  end

  def transform(type, data)
    @callback.call data
  end
end
