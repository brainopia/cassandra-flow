class Cassandra::Flow
  def self.action(klass)
    define_method klass.action_name do |*args, &block|
      new_flow        = clone
      action          = klass.new(*args, &block)
      action.location = caller.first.split(':in').first.sub(Dir.pwd + '/', '')

      new_flow.actions << action

      if klass.auto_setup?
        new_flow.setup!
        self.class.source action.target
      else
        new_flow
      end
    end
  end
end
