# Operação do protótipo local

`server --help` mostra endereço, diretório e máximo de conexões (32). Diretórios RocksDB possuem lock exclusivo: um processo por diretório. Interromper com Ctrl-C encerra o servidor; clientes podem perder respostas de commits já sincronizados, e devem tratar isso como resultado desconhecido. Não repetir escritas automaticamente.

O servidor limita mensagens a 16 MiB e startup a 10 KiB. COPY inteiro é limitado a 16 MiB. O número de conexões é limitado; conexões excedentes são encerradas. Ainda não há quotas de memória/CPU, timeout SQL, streaming de resultados, métricas de WAL/compaction ou controle de duração de consultas. Use datasets pequenos nesta versão.

O diretório de dados não admite downgrade ou migração automática. Backup online/checkpoint ainda não implementado: para cópia local, pare o servidor e copie o diretório inteiro; restaure em outro diretório e execute consultas de verificação. Esse procedimento offline não substitui o gate de backup/restore automatizado da especificação.

Logs usam tracing; `RUST_LOG=info` habilita eventos. SQL e credenciais não são registrados. Autenticação trust implica que qualquer processo local pode acessar os dados; nenhum acesso remoto é admitido pelo bind.

Build: Rust 1.92.0, C++17/libclang, RocksDB compilado do fonte. Dependências fixadas em Cargo.lock; `--locked` obrigatório em reprodução. Não existe requisito de CUDA/ROCm para build CPU.
