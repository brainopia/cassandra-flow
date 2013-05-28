class Cassandra::Flow
  def self.action(klass)
    define_method klass.action_name do |*args, &block|
      clone.tap do |it|
        it.actions << klass.new(*args, &block)
        it.setup! if klass.auto_setup?
      end
    end
  end
end
