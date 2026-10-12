# Validação inicial — 12/10/2026

Implementação: protótipo CPU P0/P1 e parte de P2. Não é a conclusão das fases P0–P7.

## Verificações executadas

- `cargo build --locked --workspace`: passou com Rust 1.92.0.
- `cargo test --locked --workspace`: passou; persistência/reabertura, agregação, falha de INSERT atômica, constraints/transações rejeitadas, COPY e fallback CPU.
- `cargo clippy --locked --workspace --all-targets -- -D warnings`: passou.
- `cargo fmt --all --check`: passou.
- `python3 tests/protocol/smoke.py`: passou; psql, framing, NULL/booleanos, erro/recuperação, rejeição de extended, COPY, ledger externo e SIGKILL/reabertura.
- `OWNSQL_PORT=55434 ruby tests/drivers/ruby_pg.rb`: passou com Ruby 4.0.6, pg 1.6.3, libpq 18.0.1. Apenas consultas simples; prepared rejeitado com 0A000.

## Comparação diferencial

PostgreSQL 17.11 (`17.11-1.pgdg12+2`), instância isolada local, locale C/UTF8; fsync, synchronous_commit e full_page_writes ligados. Pacote Debian SHA256: `cb33e6465c2b8b223ceabedfc2009bdaf7d678729b8466b43011b7914e5213c8`.

Seed 42, 10.000 linhas, perfil uniforme, strings variáveis e NULL em payload. Mesmos comandos de carga, sem PK/índices nesta etapa. Trinta amostras por consulta e motor, com aquecimento prévio: **150/150 consultas CPU iguais ao PostgreSQL**, sem erros ou retries. Resultados completos foram comparados, não somente hashes/contagens.

Dataset SHA256: `ce17a30f902059e27add0a20a0e842bf35b2920323cd8832d68a1f8864c9464d`; arquivo SQL: 793,368 bytes.

Build OwnSQL/harness **dev, sem otimização**; resultados servem para correção e diagnóstico, sem inferência sobre desempenho release. Execução sequencial local, caches aquecidos, sem isolamento de carga do SO. PostgreSQL usa seu build empacotado. Tempos incluem consumo de todo o resultado.

| Consulta | PostgreSQL p50 ms | OwnSQL CPU p50 ms | Amostras por motor | Erros CPU |
|---|---:|---:|---:|---:|
| B01 | 0.591 | 7.753 | 30 | 0 |
| B06 | 0.768 | 8.492 | 30 | 0 |
| B06-filter | 0.693 | 11.633 | 30 | 0 |
| B07 | 1.476 | 13.023 | 30 | 0 |
| B09 | 1.555 | 10.061 | 30 | 0 |

CPU foi mais lento neste smoke test. Nenhum ganho comercial ou GPU foi demonstrado. p99 não calculado (menos de 1.000 amostras). Não se trata do benchmark estatístico completo: faltam cinco rodadas independentes, release, concorrência, open-loop, intervalos de confiança, memória/disco e workloads sustentados.

Arquivos locais completos:

- [PostgreSQL](../../benchmarks/results/1791776552-postgres-319771/report.md)
- [CPU final](../../benchmarks/results/1791776736-cpu-327406/report.md)

Os diretórios de resultados são ignorados em versionamento para não misturar rodadas futuras com fonte. Este registro resume a rodada executada. Hardware e amostras estão nos manifestos/JSONL locais. Existe uma rodada CPU anterior à ativação de TCP_NODELAY; ela foi preservada, mas a tabela acima usa a versão final.

## Gates ainda não concluídos

MVCC/snapshot isolation SQL; UPDATE/DELETE e constraints; protocolo extended e codecs binários; SCRAM/TLS/cancelamento; Rails/catálogos/sequences; backup/checkpoint testado; falhas de disco/power loss; limites de memória e scan em streaming; perfis completos de benchmark; kernels CUDA/HIP; base Parquet/delta. Esses itens estão descritos na especificação original e não devem ser tratados como implementados.
