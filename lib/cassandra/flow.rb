require 'cassandra/mapper'

class Cassandra::Flow
  # hook to extend flow with additional actions
  def self.action(klass, type=nil)
    if type == :source
      define = :define_singleton_method
      parent = false
    else
      define = :define_method
      parent = true
    end
    send define, klass.action_name do |*args, &block|
      new_action = klass.new parent && action
      new_action.setup! *args, &block
      Cassandra::Flow.new new_action
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

  attr_reader :action

  def initialize(action)
    @action = action
  end
end
