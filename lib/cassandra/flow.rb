require 'cassandra/mapper'

class Cassandra::Flow
  require_relative 'flow/extend/action'
  require_relative 'flow/action'
  require_relative 'flow/action/when'

  attr_reader :actions, :source, :target

  def initialize(mapper)
    @source  = mapper
    @actions = []
  end

  def target(mapper=nil)
    return @target unless mapper
    cloned { @target = mapper }
  end

  def setup!
    source.config.dsl.after_insert {|data| propagate :insert, data }
    source.config.dsl.after_remove {|data| propagate :remove, data }
    actions.each {|it| it.setup! self }
  end

  private

  def cloned(&block)
    clone.tap {|it| it.instance_eval(&block) }
  end

  def propagate(type, record)
    records = [record]

    records = actions.inject(records) do |records, action|
      records.flat_map {|it|
        action.propagate type, it
      }.compact
    end

    records.each {|it| target.send type, it }
  end
end
