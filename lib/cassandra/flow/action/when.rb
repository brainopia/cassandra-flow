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
    actual_value = data[@field]

    if @value.is_a? Array
      @value.include? actual_value
    else
      @value == actual_value
    end
  end
end
