class Cassandra::Flow
  def self.action(klass)
    action_name = klass.name.downcase.split('::').last
    define_method action_name do |*args, &block|
      cloned do
        actions << klass.new(self, *args, &block)
      end
    end
  end

  private

  def cloned(&block)
    clone.tap {|it| it.instance_eval(&block) }
  end
end
