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

  def propagate(type, data, log)
    inject([data]) do |records, action|
      info log, action, type, records if log
      records.compact.flat_map do |it|
        action.propagate type, it
      end
    end
  end

  private

  def info(log, action, type, records)
    log.info "#{action.class}" do
      "#{type}\n" +
      "#{action.location}\n" +
      records.map {|record| "  #{record.inspect}" }.join("\n") +
      (action.class.auto_setup? ? "\n" : "")
    end
  end

  def initialize_clone(*)
    super
    @actions = @actions.map(&:clone)
  end
end
