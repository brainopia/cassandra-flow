class Cassandra::Flow::Actions
  extend Forwardable
  include Enumerable

  def_delegators :@actions, :each, :<<, :last, :index, :[]

  def initialize(actions=[])
    @actions = actions
  end

  def setup!(flow)
    each {|it| it.setup! flow }
  end

  def propagate(type, data)
    inject([data]) do |records, action|
      records.compact.flat_map do |it|
        action.propagate type, it
      end
    end
  end

  private

  def initialize_clone(*)
    super
    @actions = @actions.map(&:clone)
  end
end
