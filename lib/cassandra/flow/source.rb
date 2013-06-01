class Cassandra::Flow::Source < Cassandra::Flow
  attr_reader :source

  def initialize(mapper)
    super()
    @source = mapper
  end

  def setup!
    super
    source.config.dsl.after_insert {|data| actions.propagate :insert, data }
    source.config.dsl.after_remove {|data| actions.propagate :remove, data }
  end
end
