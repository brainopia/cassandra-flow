require 'forwardable'
require 'cassandra/mapper'

class Cassandra::Flow
  require_relative 'flow/extend/action'
  require_relative 'flow/actions'
  require_relative 'flow/action'
  require_relative 'flow/action/target'
  require_relative 'flow/action/when'
  require_relative 'flow/action/derive'
  require_relative 'flow/action/flag'

  attr_reader :source, :actions

  def initialize(mapper)
    @source  = mapper
    @actions = Actions.new
  end

  def setup!
    actions.setup! self
    start_propagation!
  end

  private

  def initialize_clone(*)
    super
    @actions = actions.clone
  end

  def start_propagation!
    source.config.dsl.after_insert {|data| actions.propagate :insert, data }
    source.config.dsl.after_remove {|data| actions.propagate :remove, data }
  end
end
