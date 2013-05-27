class Cassandra::Flow
  def self.action(klass)
    action_name = klass.name.downcase.split('::').last
    define_method action_name do |*args, &block|
      clone.tap do |it|
        it.actions << klass.new(*args, &block)
        it.setup! if klass.auto_setup?
      end
    end
  end
end
