require 'pg'
def connect
  PG.connect(host: '127.0.0.1', port: ENV.fetch('OWNSQL_PORT', '55432'), user: 'app', dbname: 'app', sslmode: 'disable')
end
connection = connect
raise 'SELECT failed' unless connection.exec('SELECT 1').values == [['1']]
raise 'parameters failed' unless connection.exec_params('SELECT $1::bigint, $2::boolean, $3::text', [42,true,nil]).values == [['42','t',nil]]
connection.prepare('numbers','SELECT $1::bigint + 1')
raise 'prepared failed' unless connection.exec_prepared('numbers',[41]).values == [['42']]
connection.exec('DROP TABLE IF EXISTS driver_stock')
connection.exec('CREATE TABLE driver_stock (id BIGINT PRIMARY KEY, stock INTEGER NOT NULL CHECK (stock >= 0))')
connection.exec('INSERT INTO driver_stock VALUES (1,100)')
connection.exec('BEGIN')
raise 'update returning failed' unless connection.exec_params('UPDATE driver_stock SET stock=stock-$1::integer WHERE id=$2::bigint AND stock >= $1::integer RETURNING stock',[2,1]).values == [['98']]
connection.exec('ROLLBACK')
raise 'rollback failed' unless connection.exec('SELECT stock FROM driver_stock').values == [['100']]
errors = Queue.new
threads = 8.times.map do
  Thread.new do
    c=connect
    5.times do
      attempts=0
      begin
        c.exec('BEGIN')
        raise 'no stock' unless c.exec_params('UPDATE driver_stock SET stock=stock-1 WHERE id=$1::bigint AND stock>=1 RETURNING stock',[1]).ntuples==1
        c.exec('COMMIT')
      rescue PG::Error => e
        c.exec('ROLLBACK')
        attempts+=1
        if e.result.error_field(PG::Result::PG_DIAG_SQLSTATE)=='40001' && attempts<100
          sleep(0.001*attempts); retry
        end
        raise
      end
    end
    c.close
  rescue => e
    errors << e
  end
end
threads.each(&:join)
raise errors.pop unless errors.empty?
raise 'lost stock update' unless connection.exec('SELECT stock FROM driver_stock').values==[['60']]
connection.exec('DROP TABLE driver_stock')
puts "PASS: Ruby #{RUBY_VERSION}, pg #{PG::VERSION}, libpq #{PG.library_version}; parameters, prepared, RETURNING, rollback and 8 concurrent writers / 40 committed sales"
connection.close
