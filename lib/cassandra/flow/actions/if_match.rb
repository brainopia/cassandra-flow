class Cassandra::Flow::Action::IfMatch < Cassandra::Flow::Action
  action!

  PRESENT_SYMBOL = :present

  def setup!(field, value=PRESENT_SYMBOL, &block)
    @field = field
    @value = value
    if block
      block.call Cassandra::Flow.new(self)
      @block_children  = @children
      @block_endpoints = endpoints @block_children
      @children        = []
    end
  end

  def propagate(type, data)
    matched = matches? data
    if @block_children and matched
      propagate_for @block_children, type, data
    elsif @block_children or matched
      propagate_next type, data
    end
  end

  def add_child(action)
    super
    return unless @block_endpoints
    @block_endpoints.each do |it|
      action.add_parent it
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
end
