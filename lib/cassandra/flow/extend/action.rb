class Cassandra::Flow
  def self.action(klass)
    action_name = klass.name.downcase.split('::').last
    define_method action_name do |*args, &block|
      cloned do
        actions << klass.new(*args, &block)
      end
    end
  end
end
