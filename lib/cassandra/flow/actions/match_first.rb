# TODO: reinsert in source (two records in catalog for one entry)
class Cassandra::Flow::Action::MatchFirst < Cassandra::Flow::Action
  action!
  attr_reader :mapper, :callback, :catalog

  def setup!(mapper, &callback)
    @mapper   = mapper
    @callback = callback

    append_name mapper.table
    build_catalog

    mapper.config.dsl.after_insert do |match|
      key    = select(:key, match)
      subkey = select(:subkey, match)

      lock key do
        records = catalog.get key, start: subkey

        records.each do |record|
          catalog.remove record
          propagate_next :remove, record[:action_result]

          record[:action_result] = callback.call record[:action_data], match
          record.merge! subkey

          catalog.insert record
          propagate_next :insert, record[:action_result]
        end
      end
    end

    mapper.config.dsl.after_remove do |match|
      key    = select(:key, match)
      subkey = select(:subkey, match)

      lock key do
        records = catalog.get key.merge(subkey)
        records.each do |record|
          catalog.remove record
          propagate_next :remove, record[:action_result]
          match_first :insert, key, record[:action_data]
        end
      end
    end
  end

  def propagate(type, data)
    key = select(:key, data)
    lock key do
      match_first type, key, data
    end
  end

  private

  def match_first(type, key, data)
    matched = mapper.one key
    subkey  = matched ? select(:subkey, matched) : max_subkey
    result  = callback.call data, matched

    if type == :insert
      catalog_record = key
      catalog_record.merge! subkey
      catalog_record.merge! action_data: data, action_result: result
      catalog.insert catalog_record
    elsif type == :remove
      found = catalog.get(key.merge(subkey)).find {|it| it[:action_data] == data }
      catalog.remove found if found
    end

    propagate_next type, result
  end

  def select(field, data)
    data.select {|k,_| mapper.config.send(field).include? k }
  end

  def max_subkey
    mapper.config.subkey.each_with_object({}) do |field, data|
      type        = mapper.config.types[field]
      data[field] = Cassandra::Mapper::Convert.max type
    end
  end

  def build_catalog
    config = mapper.config
    @catalog = Cassandra::Mapper.new keyspace_name, name do
      key *config.key
      subkey *config.subkey, :uuid
      type :action_data, :yaml
      type :action_result, :yaml
      type :uuid, :uuid

      config.subkey.each do |field|
        type field, config.types[field]
      end

      before_insert do |data|
        data[:uuid] ||= Time.now
      end
    end
  end

  def lock(key, &block)
    super name + key.values.join('.'), &block
  end
end
