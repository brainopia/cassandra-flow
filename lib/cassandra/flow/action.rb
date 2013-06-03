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

  def next_actions
    next_actions = flow.actions[flow.actions.index(self)+1..-1]
    Cassandra::Flow.new next_actions
  end

  private

  def lock(lock_name, &block)
    # FIXME
    yield
  end

  def keyspace_name
    keyspaces = Cassandra::Mapper.schema[:keyspaces]
    if keyspaces.size == 1
      keyspaces.first
    elsif keyspaces.include? :views
      :views
    else
      raise ArgumentError, 'unsupported yet'
    end
  end
end
