trap(:INT) { p Thread.main.backtrace; exit! }
class Cassandra::Flow
  def self.action(klass)
    define_method klass.action_name do |*args, &block|
      new_flow = clone
      new_flow.actions << klass.new(*args, &block)
      if klass.auto_setup?
        new_flow.setup!
        self.class.source new_flow.actions.last.target
      else
        new_flow
      end
    end
  end
end
