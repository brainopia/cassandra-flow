# TODO: reinsert?
class Cassandra::Flow::Action::MatchTime < Cassandra::Flow::Action
  action!
  attr_reader :mapper, :callback, :catalog,
              :source_field, :matched_field

  TIME_STEP = 0.001

  def setup!(mapper, source_field=nil, &callback)
    @mapper        = mapper
    @callback      = callback
    @source_field  = source_field || root.source.config.subkey.first
    @matched_field = mapper.config.subkey.first

    append_name mapper.table
    append_name source_field.to_s
    append_name matched_field.to_s
    build_catalog

    mapper.config.dsl.after_insert do |match|
      key          = select(:key, match)
      matched_time = match[matched_field]

      lock key do
        # -0.001 instead of (slice: :before) because of uuid
        records = catalog.get key, start: { source_time: matched_time - TIME_STEP }
        records.select! do |it|
          not it[:matched_time] or it[:matched_time] <= matched_time
        end

        records.each do |record|
          propagate_next :remove, record[:action_result]
          record[:action_result] = callback.call record[:action_data], match
          propagate_next :insert, record[:action_result]

          record[:matched_time] = matched_time
          catalog.insert record
        end
      end
    end

    mapper.config.dsl.after_remove do |match|
      key          = select(:key, match)
      matched_time = match[matched_field]

      lock key do
        records = catalog.get key, start: { source_time: matched_time - TIME_STEP }
        records.select! {|it| it[:matched_time].to_i == matched_time.to_i }

        records.each do |record|
          catalog.remove record
          propagate_next :remove, record[:action_result]
          match_time :insert, key, record[:action_data]
        end
      end
    end
  end

  def propagate(type, data)
    key = select(:key, data)
    return if key.values.any?(&:nil?)

    lock key do
      match_time type, key, data
    end
  end

  private

  def match_time(type, key, data)
    source_time = data[source_field]
    error! "missing :#{source_field} in #{data.inspect}" unless source_time

    matched = mapper.one key, reversed: true,
                         start: { matched_field => source_time, slice: :after }

    matched_time = matched[matched_field] if matched
    result       = callback.call data, matched

    if type == :insert
      catalog_record = key
      catalog_record.merge! \
        action_data:    data,
        action_result:  result,
        matched_time:   matched_time,
        source_time:    source_time
      catalog.insert catalog_record
    elsif type == :remove
      potential = catalog.get key,
        start:  { source_time: source_time - TIME_STEP },
        finish: { source_time: source_time + TIME_STEP }

      found = potential.find {|it| it[:action_data] == data }
      catalog.remove found if found
    end

    propagate_next type, result
  end

  def build_catalog
    config = mapper.config
    @catalog = Cassandra::Mapper.new keyspace_name, name do
      key *config.key
      subkey :source_time
      type :action_data,   :yaml
      type :action_result, :yaml
      type :source_time,   :uuid
      type :matched_time,  :time
    end
  end

  def select(field, data)
    data.select {|k,_| mapper.config.send(field).include? k }
  end

  def lock(key, &block)
    super name + key.values.join('.'), sleep: 1000, &block
  end

  def error!(text, error=ArgumentError)
    raise error, <<-ERROR
      #{text}
      from #{location}
      parents #{parents.map(&:location).join(', ')}
    ERROR
  end
end
