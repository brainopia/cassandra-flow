class Cassandra::Flow::Action
  class << self
    def action!(type=nil)
      Cassandra::Flow.action self, type
    end

    def action_name
      name.split('::').last.gsub(/(.)([A-Z])/,'\1_\2').downcase
    end
  end

  attr_reader :location, :parents, :children, :name, :suffix

  def initialize(action=nil)
    @location = caller[2].gsub(/(:in.*)|(#{Dir.pwd}\/)/, '')
    @parents  = []
    @children = []
    @name     = self.class.action_name

    if action
      @suffix = action.suffix
      append_name action.suffix
      add_parent action
    end
  end

  def add_parent(action)
    parents << action
    action.add_child self
  end

  def add_child(action)
    children << action
  end

  def propagate(type, data)
    data = transform type, data
    propagate_next type, data
  end

  def transform(type, data)
    data
  end

  def propagate_next(type, data)
    propagate_for children, type, data
  end

  def propagate_for(actions, type, data)
    log do |it|
      it.puts name
      it.puts "location - #{location}"
      it.puts "destinations - #{actions.map(&:location)}"
      it.puts type
      it.puts data.inspect
    end

    propagation = ->(it) do
      it.freeze
      actions.map {|action| action.propagate type, it }
    end

    if data.is_a? Array
      data.each(&propagation)
    elsif data
      propagation.call data
    end
  end

  def endpoints(actions=children)
    return self if actions.empty?
    actions.flat_map(&:endpoints)
  end

  def root
    return self if parents.empty?
    parents.first.root
  end

  def source
    return source if is_a? Source
    return target if is_a? Target
    parents.first.source
  end

  def inspect
    "#{self.class.name}: #{location} #{name}"
  end

  private

  def lock(lock_name, options={}, &block)
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

  def append_name(string)
    name << '_' << string if string
  end

  def log(&block)
    logger = Cassandra::Flow.logger
    if logger
      block.call logger
      logger.puts "\n"*2
    end
  end

  def log_inspect(entry)
    log {|it| it.puts "inspect: #{entry.inspect}" }
  end
end
