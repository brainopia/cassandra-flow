class Cassandra::Flow::Action::If < Cassandra::Flow::Action
  PRESENT_SYMBOL = :present

  def initialize(field, value=PRESENT_SYMBOL)
    @field = field
    @value = value
  end

  def propagate(type, data)
    data if matches? data
  end

  private

  def matches?(data)
    actual_value = data[@field]

    case @value
    when Array
      @value.include? actual_value
    when PRESENT_SYMBOL
      actual_value
    else
      @value == actual_value
    end
  end
end
