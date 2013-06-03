class Cassandra::Flow::Action
  class << self
    def inherited(klass)
      Cassandra::Flow.action klass
    end

    def auto_setup!
      @auto_setup = true
    end

    def auto_setup?
      @auto_setup
    end

    def action_name
      name.split('::').last.gsub(/(.)([A-Z])/,'\1_\2').downcase
    end
  end

  attr_accessor :location

  def setup!(flow)
  end

  def lock(lock_name, &block)
    # FIXME
    yield
  end

  def next_actions
    next_actions = flow.actions[flow.actions.index(self)+1..-1]
    Cassandra::Flow::Actions.new next_actions
  end
end
