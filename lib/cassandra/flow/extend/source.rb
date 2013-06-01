class Cassandra::Flow
  def self.source(mapper)
    Source::new(mapper)
  end
end
