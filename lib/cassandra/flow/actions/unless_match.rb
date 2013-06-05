class Cassandra::Flow::Action::UnlessMatch < Cassandra::Flow::Action::IfMatch
  action!

  def matches?(*)
    not super
  end
end
