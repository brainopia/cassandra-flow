class Cassandra::Flow::Action::Target < Cassandra::Flow::Action
  action!
  attr_reader :target

  def setup!(mapper)
    @target = mapper
  end

  def transform(type, data)
    @target.send type, data
  end
end
