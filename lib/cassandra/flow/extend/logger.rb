class Cassandra::Flow
  class << self
    @@logger = nil

    def logger
      @@logger
    end

    def logger=(object)
      @@logger = object.is_a?(IO) ? Logger.new(object) : object
    end
  end

  def logger
    @@logger
  end
end
