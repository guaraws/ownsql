# Compatibilidade 0.1

| Área | Estado |
|---|---|
| Startup v3, SSLRequest/GSS recusados, AuthenticationOk, ParameterStatus, ReadyForQuery | Implementado; somente loopback/trust |
| Simple Query, texto RowDescription/DataRow, erros | Implementado; uma instrução por Query |
| psql consultas comuns / COPY texto | Teste reproduzível em `tests/protocol/smoke.py` |
| tokio-postgres 0.7.15 simple_query | Harness |
| Extended Parse/Bind/Execute/Describe | Erro 0A000, recupera em Sync |
| SCRAM, TLS, CancelRequest, BackendKeyData | Pendente |
| CREATE TABLE / DROP TABLE / INSERT VALUES e SELECT | Subconjunto analítico |
| COUNT/SUM/MIN/MAX/AVG, filtros, joins CPU, GROUP BY, ORDER BY, LIMIT | DataFusion; conjunto diferencial inicial no harness |
| smallint/integer/bigint, bool, text, real/double, date, timestamp sem fuso | Tabelas e formato texto |
| bigint resultado COUNT | Arrow UInt64 anunciado como PG bigint; acima de i64 rejeitado no codec |
| numeric, bytea, uuid, timestamptz, varchar limitado, codecs binários | Pendente; não aceitar DDL |
| Constraints, defaults, UPDATE/DELETE/RETURNING/ON CONFLICT | Erro explícito |
| BEGIN/COMMIT/ROLLBACK/savepoints/isolation levels | Erro explícito |
| COPY FROM STDIN texto sem opções/colunas | Até 16 MiB por carga, NULL `\N`, escapes `\t`, `\n`, `\r`, `\\` |
| COPY CSV/binário/OUT | Erro explícito |
| SET/SHOW/pg_catalog/information_schema/Rails migrations | Pendente |
| Ruby 4.0.6 / pg 1.6.3 / libpq 18.0.1 | SELECT/NULL simples e rejeição de prepared verificados; teste em `tests/drivers/ruby_pg.rb` |
| psycopg, ActiveRecord e Ruby pg completo | Gate pendente; compatibilidade não anunciada |

O servidor publica `server_version=0.1.0`, versão do produto, sem anunciar PostgreSQL completo. SQLSTATE 0A000 para funções do produto não implementadas e 42601 para parse; erros internos do DataFusion ainda usam XX000, sem mapeamento detalhado para todos os SQLSTATE PostgreSQL.
