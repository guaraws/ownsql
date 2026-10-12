mod transactional;
use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Generate {
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value_t = 100000)]
        rows: usize,
        #[arg(long, default_value = "benchmarks/data")]
        output: PathBuf,
    },
    Load {
        #[arg(long, default_value = "cpu")]
        engine: String,
        #[arg(long, default_value = "benchmarks/data/events.sql")]
        input: PathBuf,
    },
    Run {
        #[arg(long, default_value = "analytical")]
        profile: String,
        #[arg(long, default_value = "cpu")]
        engine: String,
        #[arg(long, default_value_t = 30)]
        samples: usize,
        #[arg(long, default_value = "benchmarks/data/manifest.json")]
        dataset: PathBuf,
        #[arg(long, default_value = "benchmarks/results")]
        output: PathBuf,
        #[arg(long)]
        baseline: Option<PathBuf>,
    },
    Transactions {
        #[arg(long, default_value = "cpu")]
        engine: String,
        #[arg(long, default_value_t = 8)]
        clients: usize,
        #[arg(long, default_value_t = 300)]
        seconds: u64,
        #[arg(long, default_value_t = 60)]
        warmup: u64,
        #[arg(long)]
        rate: Option<f64>,
        #[arg(long, default_value = "benchmarks/results")]
        output: PathBuf,
    },
    Report {
        #[arg(long, default_value = "benchmarks/results")]
        input: PathBuf,
    },
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn query_set() -> Vec<(&'static str, &'static str)> {
    vec![
        ("B01", "SELECT * FROM events WHERE id = 42"),
        ("B06", "SELECT COUNT(*), SUM(amount_cents) FROM events"),
        (
            "B06-filter",
            "SELECT SUM(amount_cents) FROM events WHERE tenant_id = 7 AND status = 1",
        ),
        (
            "B07",
            "SELECT category_id, COUNT(*), SUM(amount_cents) FROM events GROUP BY category_id ORDER BY category_id",
        ),
        (
            "B09",
            "SELECT id, amount_cents FROM events ORDER BY amount_cents DESC, id LIMIT 100",
        ),
    ]
}
async fn connect(engine: &str) -> Result<Client> {
    ensure!(
        ["cpu", "postgres"].contains(&engine),
        "GPU backend not implemented"
    );
    let url = std::env::var("OWNSQL_BENCH_URL").unwrap_or_else(|_| {
        format!(
            "host=127.0.0.1 port={} user=bench dbname=bench sslmode=disable",
            if engine == "cpu" { 55432 } else { 5432 }
        )
    });
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    Ok(client)
}
fn generate(seed: u64, rows: usize, output: &Path) -> Result<()> {
    ensure!(rows > 0, "rows must be positive");
    fs::create_dir_all(output)?;
    let file = File::create(output.join("events.sql"))?;
    let mut w = BufWriter::new(file);
    writeln!(w, "DROP TABLE IF EXISTS events;")?;
    writeln!(
        w,
        "CREATE TABLE events (id BIGINT, tenant_id INTEGER, customer_id BIGINT, category_id INTEGER, occurred_at TIMESTAMP, amount_cents BIGINT, status SMALLINT, payload TEXT);"
    )?;
    let mut state = seed ^ 0x9e3779b97f4a7c15;
    for start in (1..=rows).step_by(1000) {
        write!(w, "INSERT INTO events VALUES ")?;
        for id in start..=rows.min(start + 999) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            if id != start {
                write!(w, ",")?;
            }
            let payload = if id % 17 == 0 {
                "NULL".to_string()
            } else {
                format!("'event-{id}-{}'", "x".repeat((state % 40) as usize))
            };
            write!(
                w,
                "({id},{},{},{},'2026-01-01 00:00:00',{}, {},{payload})",
                state % 100,
                state % 10000,
                state % 10,
                state % 100000,
                state % 3
            )?;
        }
        writeln!(w, ";")?;
    }
    w.flush()?;
    let bytes = fs::read(output.join("events.sql"))?;
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(
            &json!({"seed":seed,"rows":rows,"profile":"uniform","sha256":hash(&bytes),"bytes":bytes.len(),"format":"ownsql-bench-v1","constraints":"none; P1/P2 CPU baseline"}),
        )?,
    )?;
    println!(
        "Dataset: {} rows, {} bytes, {}",
        rows,
        bytes.len(),
        output.display()
    );
    Ok(())
}
fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1)]
}
fn report(dir: &Path) -> Result<()> {
    if !dir.join("samples.jsonl").exists() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() && path.join("samples.jsonl").exists() {
                report(&path)?;
            }
        }
        return Ok(());
    }
    let mut groups = std::collections::BTreeMap::<String, (Vec<f64>, usize)>::new();
    for line in BufReader::new(File::open(dir.join("samples.jsonl"))?).lines() {
        let v: Value = serde_json::from_str(&line?)?;
        let group = groups
            .entry(v["query_id"].as_str().context("query_id")?.into())
            .or_default();
        if v["success"] == true {
            group.0.push(v["total_ms"].as_f64().context("total_ms")?);
        } else {
            group.1 += 1;
        }
    }
    let mut csv = String::from("query,successful_ops,errors,p50_ms,p95_ms,p99_ms\n");
    let mut md = String::from(
        "# OwnSQL — relatório de execução\n\nLatência inclui envio e consumo completo do resultado pelo cliente. Execução sequencial local; não mede carga concorrente. Artefatos de build e ambiente constam no manifesto. Sem afirmação de superioridade.\n\n| Consulta | Sucessos | Erros | p50 ms | p95 ms | p99 ms |\n|---|---:|---:|---:|---:|---:|\n",
    );
    for (id, (mut times, errors)) in groups {
        times.sort_by(f64::total_cmp);
        let p50 = if times.is_empty() {
            "N/A".into()
        } else {
            format!("{:.3}", percentile(&times, 0.5))
        };
        let p95 = if times.is_empty() {
            "N/A".into()
        } else {
            format!("{:.3}", percentile(&times, 0.95))
        };
        let p99 = if times.len() < 1000 {
            "N/A".into()
        } else {
            format!("{:.3}", percentile(&times, 0.99))
        };
        csv.push_str(&format!(
            "{id},{},{errors},{p50},{p95},{p99}\n",
            times.len()
        ));
        md.push_str(&format!(
            "| {id} | {} | {errors} | {p50} | {p95} | {p99} |\n",
            times.len()
        ));
    }
    fs::write(dir.join("summary.csv"), csv)?;
    fs::write(dir.join("report.md"), md)?;
    println!("Report: {}", dir.join("report.md").display());
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Generate { seed, rows, output } => generate(seed, rows, &output)?,
        Command::Load { engine, input } => {
            let client = connect(&engine).await?;
            let start = Instant::now();
            for line in BufReader::new(File::open(input)?).lines() {
                client.simple_query(&line?).await?;
            }
            println!("Loaded in {:.3}s", start.elapsed().as_secs_f64());
        }
        Command::Report { input } => report(&input)?,
        Command::Transactions {
            engine,
            clients,
            seconds,
            warmup,
            rate,
            output,
        } => transactional::run(engine, clients, seconds, warmup, rate, &output).await?,
        Command::Run {
            profile,
            engine,
            samples,
            dataset,
            output,
            baseline,
        } => {
            ensure!(
                profile == "analytical",
                "only analytical profile is implemented"
            );
            ensure!(samples >= 1, "samples must be positive");
            let dataset: Value = serde_json::from_slice(&fs::read(dataset)?)?;
            let baseline: Option<Value> = baseline
                .map(|p| -> Result<Value> {
                    Ok(serde_json::from_slice(&fs::read(p.join("results.json"))?)?)
                })
                .transpose()?;
            let client = connect(&engine).await?;
            let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let run_id = format!("{epoch}-{engine}-{}", std::process::id());
            let dir = output.join(&run_id);
            fs::create_dir_all(&dir)?;
            let version = if engine == "postgres" {
                format!("{:?}", client.simple_query("SELECT version()").await?)
            } else {
                "OwnSQL 0.1.0".into()
            };
            let queries = query_set();
            let manifest = json!({"run_id":run_id,"utc_unix_seconds":epoch,"engine":engine,"driver":"tokio-postgres 0.7.15 simple query","version":version,"dataset":dataset,"clients":1,"cache_mode":"warm after one unmeasured query","durability":"synchronous WAL; verify baseline server configuration","build":if cfg!(debug_assertions){"dev"}else{"release"},"hardware":fs::read_to_string("/proc/cpuinfo").unwrap_or_default(),"memory":fs::read_to_string("/proc/meminfo").unwrap_or_default(),"query_hash":hash(queries.iter().map(|q| q.1).collect::<Vec<_>>().join("\n").as_bytes()),"scope":"correctness and local analytical smoke; full specification benchmark gates pending"});
            fs::write(
                dir.join("manifest.json"),
                serde_json::to_vec_pretty(&manifest)?,
            )?;
            let mut log = BufWriter::new(File::create(dir.join("samples.jsonl"))?);
            let mut results = serde_json::Map::new();
            let mut failures = 0;
            for (id, sql) in queries {
                let _warmup = client.simple_query(sql).await;
                let mut expected: Option<Value> = None;
                for iteration in 0..samples {
                    let start = Instant::now();
                    let response = client.simple_query(sql).await;
                    let elapsed = start.elapsed().as_secs_f64() * 1000.;
                    let mut sample = json!({"run_id":run_id,"query_id":id,"iteration":iteration,"total_ms":elapsed,"execution_ms":null,"H2D_ms":null,"kernel_ms":null,"D2H_ms":null,"retries":0});
                    match response {
                        Ok(messages) => {
                            let rows: Vec<Vec<Option<String>>> = messages
                                .iter()
                                .filter_map(|m| match m {
                                    SimpleQueryMessage::Row(r) => Some(
                                        (0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect(),
                                    ),
                                    _ => None,
                                })
                                .collect();
                            let value = json!(rows);
                            let stable = expected.as_ref().is_none_or(|e| e == &value);
                            let matches_baseline =
                                baseline.as_ref().is_none_or(|b| b.get(id) == Some(&value));
                            sample["success"] = json!(stable && matches_baseline);
                            sample["rows_returned"] = json!(rows.len());
                            sample["result_hash"] =
                                json!(hash(serde_json::to_string(&value)?.as_bytes()));
                            if !stable || !matches_baseline {
                                sample["error"] = json!("result mismatch");
                                failures += 1;
                            }
                            expected = Some(value.clone());
                            results.insert(id.into(), value);
                        }
                        Err(e) => {
                            sample["success"] = json!(false);
                            sample["error"] = json!(e.to_string());
                            failures += 1;
                        }
                    }
                    writeln!(log, "{}", serde_json::to_string(&sample)?)?;
                    log.flush()?;
                }
            }
            fs::write(
                dir.join("results.json"),
                serde_json::to_vec_pretty(&results)?,
            )?;
            report(&dir)?;
            ensure!(
                failures == 0,
                "{failures} failed samples; retained in {}",
                dir.display()
            );
        }
    }
    Ok(())
}
