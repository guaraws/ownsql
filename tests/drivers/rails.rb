$LOAD_PATH.unshift(File.expand_path('../../integrations/rails/lib',__dir__))
require 'active_record'
require 'active_record/connection_adapters/ownsql_adapter'
require 'logger'
require 'fileutils'
raise 'test requires pinned ActiveRecord 8.1.3.1' unless ActiveRecord::VERSION::STRING=='8.1.3.1'
# Captures the actual SQL emitted, including connection setup and introspection.
FileUtils.mkdir_p(File.expand_path('../../benchmarks/results',__dir__))
ActiveRecord::Base.logger=Logger.new(File.expand_path('../../benchmarks/results/rails-sql.log',__dir__))
ActiveRecord::Base.establish_connection(adapter:'ownsql',host:'127.0.0.1',port:ENV.fetch('OWNSQL_PORT','55432'),database:'app',username:'app',sslmode:'disable',pool:4)
class RailsProduct < ActiveRecord::Base; self.table_name='rails_products'; end
c=ActiveRecord::Base.connection
c.execute('DROP TABLE IF EXISTS rails_products')
c.execute('CREATE TABLE rails_products (id BIGINT PRIMARY KEY, stock INTEGER NOT NULL CHECK(stock>=0), label TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)')
raise 'schema discovery failed' unless c.columns('rails_products').map(&:name)==%w[id stock label created_at updated_at]
raise 'PK discovery failed' unless c.primary_key('rails_products')=='id'
p=RailsProduct.create!(id:1,stock:10,label:'hello')
raise 'insert/read failed' unless RailsProduct.find(1).stock==10
p.update!(stock:9)
raise 'update failed' unless p.reload.stock==9
RailsProduct.transaction do
  p.update!(stock:5)
  raise ActiveRecord::Rollback
end
raise 'rollback failed' unless p.reload.stock==9
RailsProduct.transaction do
  RailsProduct.transaction(requires_new:true) do
    p.update!(stock:2)
    raise ActiveRecord::Rollback
  end
end
raise 'savepoint failed' unless p.reload.stock==9
raise 'timestamps failed' unless p.created_at.is_a?(Time)
threads=4.times.map { Thread.new { ActiveRecord::Base.connection_pool.with_connection { raise 'pool failed' unless RailsProduct.find(1).id==1 } } }
threads.each(&:value)
p.destroy!
raise 'delete failed' unless RailsProduct.count==0
c.execute('DROP TABLE rails_products')
puts "PASS: ActiveRecord #{ActiveRecord::VERSION::STRING}, OwnSQL adapter; schema/PK, CRUD, timestamps, transactions, savepoints and pool"
