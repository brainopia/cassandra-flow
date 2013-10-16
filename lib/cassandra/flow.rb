require 'cassandra/mapper'

class Cassandra::Flow
  class << self
    attr_accessor :logger
    attr_accessor :keyspace

    # hook to extend flow with additional actions
    def action(klass, type=nil)
      if type == :source
        define   = :define_singleton_method
        instance = false
      else
        define   = :define_method
        instance = true
      end
      send define, klass.action_name do |*args, &block|
        new_action = klass.new instance && action
        new_action.setup! *args, &block

        if instance
          # to support inheritance of extended modules
          clone.tap {|it| it.action = new_action }
        else
          Cassandra::Flow.new new_action
        end
      end
    end
  end

  require_relative 'flow/action'
  require_relative 'flow/actions/source'
  require_relative 'flow/actions/target'
  require_relative 'flow/actions/derive'
  require_relative 'flow/actions/check'
  require_relative 'flow/actions/label'
  require_relative 'flow/actions/aggregate'
  require_relative 'flow/actions/flag'
  require_relative 'flow/actions/match_first'
  require_relative 'flow/actions/match_time'
  require_relative 'flow/actions/if_match'
  require_relative 'flow/actions/unless_match'

  attr_accessor :action

  def initialize(action)
    @action = action
  end

  def apply
    yield self if block_given?
  end
end
