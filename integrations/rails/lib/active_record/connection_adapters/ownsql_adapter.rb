# OwnSQL capabilities are explicit. No fabricated PostgreSQL version or catalogs.
require 'active_record/connection_adapters/postgresql_adapter'
module ActiveRecord
  module ConnectionAdapters
    class OwnsqlAdapter < PostgreSQLAdapter
      ADAPTER_NAME = 'OwnSQL'
      def adapter_name = ADAPTER_NAME
      def check_version
        raise 'OwnSQL 0.2.x required' unless (200...300).cover?(get_database_version)
      end
      def supports_advisory_locks? = false
      def supports_extensions? = false
      def supports_insert_on_conflict? = false
      def supports_insert_on_duplicate_skip? = false
      def supports_insert_on_duplicate_update? = false
      def supports_foreign_keys? = false
      def supports_indexes_in_create? = false
      def supports_check_constraints? = true
      def supports_savepoints? = true
      def supports_transaction_isolation? = false
      def max_identifier_length = 63
      def schema_search_path = 'public'
      def current_schema = 'public'
      def tables = select_values('SHOW OWNSQL TABLES')
      alias data_sources tables
      def data_source_exists?(name) = tables.include?(name.to_s)
      alias table_exists? data_source_exists?
      def views = []
      def indexes(table_name) = []
      def primary_keys(table_name) = table_info(table_name).select { |r| r['primary_key']=='t' }.map { |r| r['name'] }
      def pk_and_sequence_for(table_name) = [primary_keys(table_name).first, nil]
      def columns(table_name)
        table_info(table_name).map do |r|
          sql=r['sql_type']; oid=r['oid'].to_i
          type=case oid; when 20,21,23 then :integer; when 16 then :boolean; when 700,701 then :float; when 1700 then :decimal; when 1082 then :date; when 1114 then :datetime; else :text; end
          p,s=sql[/\((\d+),(\d+)\)/,1]&.to_i,sql[/\((\d+),(\d+)\)/,2]&.to_i
          metadata=SqlTypeMetadata.new(sql_type:sql,type:type,limit:{20=>8,21=>2,23=>4}[oid],precision:p,scale:s)
          PostgreSQL::Column.new(r['name'],get_oid_type(oid,-1,r['name'],sql),nil,PostgreSQL::TypeMetadata.new(metadata,oid:oid,fmod:-1),r['nullable']=='t')
        end
      end
      private
      def table_info(name) = select_all("SHOW OWNSQL COLUMNS #{quote_table_name(name)}").to_a
      def configure_connection
        check_version
        @raw_connection.set_client_encoding('UTF8')
        add_pg_encoders
        add_pg_decoders
        reload_type_map
        @use_insert_returning = true
      end
      def reconfigure_connection_timezone; end
      def initialize_type_map(m=type_map)
        self.class.initialize_type_map(m)
        {21=>'int2',23=>'int4',20=>'int8',700=>'float4',701=>'float8',25=>'text',16=>'bool',1082=>'date',1700=>'numeric'}.each { |oid,name| m.alias_type(oid,name) }
        m.register_type(1114,PostgreSQL::OID::Timestamp.new(timezone: :utc))
      end
      def add_pg_decoders
        map=PG::TypeMapByOid.new
        map.default_type_map=PG::TypeMapAllStrings.new
        {21=>PG::TextDecoder::Integer,23=>PG::TextDecoder::Integer,20=>PG::TextDecoder::Integer,16=>PG::TextDecoder::Boolean,700=>PG::TextDecoder::Float,701=>PG::TextDecoder::Float,1700=>PG::TextDecoder::Numeric,1114=>PG::TextDecoder::TimestampUtc}.each { |oid,klass| map.add_coder(klass.new(oid:oid)) }
        @raw_connection.type_map_for_results=map
        @type_map_for_results=map
      end
    end
  end
end
ActiveRecord::ConnectionAdapters.register('ownsql','ActiveRecord::ConnectionAdapters::OwnsqlAdapter','active_record/connection_adapters/ownsql_adapter')
