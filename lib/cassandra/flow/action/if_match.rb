class Cassandra::Flow::Action::IfMatch < Cassandra::Flow::Action
  PRESENT_SYMBOL = :present

  def initialize(field, value=PRESENT_SYMBOL, &block)
    @field   = field
    @value   = value
    @subflow = create_subflow block if block
  end

  def setup!(flow)
    if @subflow
      @subflow.parent = flow
      @subflow.setup!
    end
  end

  def propagate(type, data)
    if matches? data
      @subflow ? @subflow.propagate(type, data) : data
    else
      data if @subflow
    end
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

  def create_subflow(block)
    Cassandra::Flow.new.instance_eval(&block)
  end
end
