class Cassandra::Flow::Action::Target < Cassandra::Flow::Action
  action!

  def setup!(mapper)
    @target = mapper
  end

  def transform(type, data)
    @target.send type, data
  end
end
