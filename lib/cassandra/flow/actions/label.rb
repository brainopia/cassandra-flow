class Cassandra::Flow::Action::Label < Cassandra::Flow::Action
  action!

  def setup!(suffix)
    if @suffix
      @suffix += '_' + suffix.to_s
    else
      @suffix = suffix.to_s
    end
  end
end
