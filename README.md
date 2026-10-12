# OwnSQL

Protótipo analítico de um banco Rust, baseado em [especificacao-banco-rust-sql-gpu.md](especificacao-banco-rust-sql-gpu.md). DataFusion 50.3.0 / Arrow 56.2.1 executam SQL; RocksDB 10.4.2 via TransactionDB guarda catálogo e lotes Arrow em commits com WAL síncrono. Não exige GPU.

## Executar

Requisitos: Rust 1.92.0, compilador C/C++, libclang e espaço para compilar DataFusion/RocksDB. Em Debian/Ubuntu: `build-essential clang libclang-dev`. O primeiro build compila RocksDB e demora alguns minutos.

```sh
cargo build --locked --workspace
RUST_LOG=info cargo run --locked -p server -- --data-dir data --listen 127.0.0.1:55432
```

Em outro terminal:

```sh
PGSSLMODE=disable psql -h 127.0.0.1 -p 55432 -U app -d app
```

Envie um comando por consulta:

```sql
CREATE TABLE events (id BIGINT, tenant_id INTEGER, amount_cents BIGINT);
INSERT INTO events VALUES (1, 7, 1200), (2, 7, 2300);
SELECT tenant_id, COUNT(*), SUM(amount_cents) FROM events GROUP BY tenant_id;
```

O servidor aceita apenas loopback, com autenticação local trust. Usuário e database de startup são rótulos de conexão: há um único banco por diretório. Não há isolamento entre databases ou autorização por usuário. Não expor por proxy a redes externas.

## Validar

```sh
cargo test --locked --workspace
python3 tests/protocol/smoke.py
# Com servidor em execução e gem pg instalada:
ruby tests/drivers/ruby_pg.rb
cargo clippy --locked --workspace --all-targets -- -D warnings
```

O teste de protocolo usa o binário `target/debug/server` e psql, cria um diretório temporário, confirma escritas, encerra com SIGKILL e verifica a recuperação. Não equivale a teste de perda de energia.

## Dados e comparação

```sh
cargo run --release --locked -p bench -- generate --seed 42 --rows 100000
cargo run --release --locked -p bench -- load --engine cpu
cargo run --release --locked -p bench -- run --profile analytical --engine cpu
OWNSQL_BENCH_URL='host=127.0.0.1 port=5432 user=bench dbname=bench' cargo run --release --locked -p bench -- load --engine postgres
OWNSQL_BENCH_URL='host=127.0.0.1 port=5432 user=bench dbname=bench' cargo run --release --locked -p bench -- run --profile analytical --engine postgres
cargo run --release --locked -p bench -- run --engine cpu --baseline benchmarks/results/ID-DA-RODADA-POSTGRES
cargo run --release --locked -p bench -- report --input benchmarks/results
```

`load` substitui a tabela `events` no banco escolhido. O harness usa consultas simples para os dois motores, consome os resultados, retém falhas e grava manifesto, amostras, resumo CSV, resultados e relatório. Comparação diferencial usa `--baseline`. p99 fica ausente com menos de mil amostras. Credenciais vêm do ambiente e não são gravadas. Esse harness inicial cobre B01/B06/B07/B09 sequenciais; não implementa ainda os perfis concorrentes, intervalos de confiança e instrumentação completa da especificação.

Leia [compatibilidade](docs/compatibility/matrix.md), [decisões](docs/architecture/adr-001-prototype.md) e [operação](docs/operations/local.md) antes de ampliar o uso. A conclusão do roadmap, Rails, MVCC e aceleração GPU ainda depende das fases posteriores.

Resultados desta implementação: [validação inicial](docs/operations/validation.md), incluindo comparação diferencial e regressões observadas.
