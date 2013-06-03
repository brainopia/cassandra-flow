require 'forwardable'
require 'logger'
require 'cassandra/mapper'

class Cassandra::Flow
  require_relative 'flow/extend/action'
  require_relative 'flow/extend/source'
  require_relative 'flow/extend/logger'
  require_relative 'flow/actions'
  require_relative 'flow/source'
  require_relative 'flow/action'
  require_relative 'flow/action/target'
  require_relative 'flow/action/check'
  require_relative 'flow/action/notify'
  require_relative 'flow/action/if_match'
  require_relative 'flow/action/unless_match'
  require_relative 'flow/action/derive'
  require_relative 'flow/action/flag'
  require_relative 'flow/action/aggregate'
  require_relative 'flow/action/match_first'
  require_relative 'flow/action/match_time'

  attr_reader :actions

  def initialize(&block)
    @actions = Actions.new
  end

  def setup!(flow=self)
    actions.setup! flow
  end

  def propagate(type, data)
    actions.propagate type, data, logger
  end

  private

  def initialize_clone(*)
    super
    @actions = actions.clone
  end
end
