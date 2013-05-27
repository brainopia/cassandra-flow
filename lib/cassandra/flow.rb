require 'cassandra/mapper'

class Cassandra::Flow
  require_relative 'flow/extend/action'
  require_relative 'flow/action'
  require_relative 'flow/action/target'
  require_relative 'flow/action/when'
  require_relative 'flow/action/derive'

  attr_reader :actions, :source

  def initialize(mapper)
    @source  = mapper
    @actions = []
  end

  def setup!
    setup_callbacks!
    actions.each(&:setup!)
  end

  private

  def setup_callbacks!
    source.config.dsl.after_insert {|data| propagate :insert, data }
    source.config.dsl.after_remove {|data| propagate :remove, data }
  end

  def propagate(type, record)
    actions.inject([record]) do |records, action|
      records.compact.flat_map do |it|
        action.propagate type, it
      end
    end
  end
end
