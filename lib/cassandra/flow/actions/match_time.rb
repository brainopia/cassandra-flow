# TODO: reinsert?
# if matched mapper uses uuid we have to use TIME_STEP also
class Cassandra::Flow::Action::MatchTime < Cassandra::Flow::Action
  action!
  attr_reader :mapper, :callback, :catalog,
              :source_field, :matched_field

  # TIME_STEP instead of (slice: :before/after) because of uuid
  TIME_STEP = 0.001

  def setup!(mapper, options={}, &callback)
    @mapper        = mapper
    @callback      = callback
    @source_field  = options[:source] || source.config.subkey.first
    @matched_field = mapper.config.subkey.first
    @match_after   = options[:after]

    append_name mapper.table
    append_name source_field.to_s
    append_name matched_field.to_s
    build_catalog

    mapper.config.dsl.after_insert do |match|
      key          = select(:key, match)
      matched_time = match[matched_field]

      lock key do
        if match_after?
          records = catalog.get key, finish: { source_time: matched_time + TIME_STEP }
          records.select! do |it|
            not it[:matched_time] or it[:matched_time] > matched_time
          end
        else
          records = catalog.get key, start: { source_time: matched_time - TIME_STEP }
          records.select! do |it|
            not it[:matched_time] or it[:matched_time] < matched_time
          end
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
        query = if match_after?
          { finish: { source_time: matched_time + TIME_STEP }}
        else
          { start: { source_time: matched_time - TIME_STEP }}
        end

        records = catalog.get key, query
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

    if key.values.any?(&:nil?)
      propagate_next type, callback.call(data, nil)
    else
      lock key do
        match_time type, key, data
      end
    end
  end

  private

  def match_time(type, key, data)
    source_time = data[source_field]
    error! "missing :#{source_field} in #{data.inspect}" unless source_time

    if type == :insert
      query = if match_after?
          { start: { matched_field => source_time }}
        else
          # slice :after in reverse means to match including current record
          { reversed: true, start: { matched_field => source_time, slice: :after }}
        end
      matched = mapper.one key, query

      matched_time = matched[matched_field] if matched
      result       = callback.call data, matched

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

      if found
        result = found[:action_result]
        catalog.remove found
      end
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

  def select(key_type, data)
    mapper.config.send(key_type).each_with_object({}) do |field, result|
      result[field] = data[field]
    end
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

  def match_after?
    @match_after
  end
end
