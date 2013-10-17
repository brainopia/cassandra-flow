class Cassandra::Flow::Error < StandardError
  def initialize(location, error)
    @locations = [format(location)]
    @backtrace = error.backtrace
    @message   = error.message
  end

  def backtrace
    @locations + ['(backtrace):0'] + @backtrace
  end

  def prepend_location(location)
    @locations.push format location
  end

  private

  def format(location)
    './' + location
  end
end
