class Cassandra::Flow::Action::When < Cassandra::Flow::Action
  def initialize(field, value)
    @field = field
    @value = value
  end

  def propagate(type, data)
    data if matches? data
  end

  private

  def matches?(data)
    data[@field] == @value
  end
end
