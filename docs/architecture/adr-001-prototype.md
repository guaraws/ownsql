# ADR 001 — protótipo P0–P2

Status: implementado parcialmente, 12/10/2026.

## Execução e armazenamento

O workspace separa servidor, executor SQL/Arrow, storage, contrato de despacho GPU e harness. Parser e execução são DataFusion 50.3.0 com sqlparser 0.58.0. Arrow 56.2.1 é fixado pelo Cargo.lock. O wire simples é uma implementação Rust limitada ao subconjunto testado do protocolo v3.

Cada leitura captura imagens de tabelas confirmadas e monta MemTables em um SessionContext privado. Escritas são serializadas, executadas em outro contexto privado e persistidas em uma única transação RocksDB com `sync=true` antes de publicar as imagens e confirmar ao cliente. I/O de commit ocorre em tarefa bloqueante. Erro de execução ou persistência não publica alterações. Perda da conexão após commit implica resultado desconhecido para o cliente.

O formato `ownsql-arrow-v1` contém `table/<nome>` com IPC Arrow, incluindo schema, além do marcador de formato. Substituir todas as imagens na mesma transação garante atomicidade de catálogo/dados nesse protótipo. Versões desconhecidas são rejeitadas. Não existe WAL próprio.

Esta representação é deliberadamente inicial: regrava tabelas, clona imagens e materializa resultados em RAM. Não é o layout de chaves versionadas MVCC especificado para P3, não tem índices nem GC. Reabertura recupera imagens via WAL do RocksDB. P3 deve substituir o layout mediante migração explícita, não presumir compatibilidade de disco.

## Contrato SQL

DDL, INSERT e SELECT passam por allowlist; constraints, defaults e extensões de CREATE TABLE são rejeitados. Ainda não há transações SQL. Cada statement suportado é atômico. Múltiplos statements em uma Query são rejeitados para evitar prometer uma transação implícita inexistente.

NULL, agregações, ordenação e operações são DataFusion. Compatibilidade PostgreSQL é demonstrada caso a caso por comparação, não assumida para aritmética, overflow, casts e funções arbitrárias. Collation segue o motor, não locales PostgreSQL. Strings UTF-8. Tipo timestamp sem fuso; nenhum timestamptz anunciado.

## GPU

`gpu-api` define seleção CPU e fallback para dispositivos indisponíveis. Nenhum kernel nem capacidade CUDA/HIP anunciada. O harness rejeita engines GPU. P5/P6 exigem SDK/hardware e testes reais; placeholders não seriam evidência de aceleração.

## Próximos gates

1. P2 completo: backup consistente/restore, testes de I/O e power loss, carga maior e limites de memória.
2. P3: storage versionado MVCC, constraints, conflitos, DML e protocolo extended.
3. P4: capturar SQL de Ruby/Rails e validar versões fixadas.
4. P5/P6: kernels reais e igualdade CPU/GPU com medição end-to-end.
5. P7: Parquet/base/delta e provider de scan sem materialização integral.
