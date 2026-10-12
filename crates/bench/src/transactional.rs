use super::{connect, hash, report};
use anyhow::{Result, ensure};
use serde_json::json;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::SimpleQueryMessage;

pub async fn run(
    engine: String,
    clients: usize,
    seconds: u64,
    warmup: u64,
    rate: Option<f64>,
    output: &Path,
) -> Result<()> {
    ensure!(
        clients > 0 && clients <= 64 && seconds > 0,
        "clients=1..64, seconds>0 required"
    );
    if let Some(rate) = rate {
        ensure!(rate.is_finite() && rate > 0., "rate must be positive");
    }
    let setup = connect(&engine).await?;
    for table in ["bench_items", "bench_orders", "bench_products"] {
        setup
            .simple_query(&format!("DROP TABLE IF EXISTS {table}"))
            .await?;
    }
    for sql in [
        "CREATE TABLE bench_products (id BIGINT PRIMARY KEY, stock BIGINT NOT NULL CHECK(stock >= 0))",
        "CREATE TABLE bench_orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL)",
        "CREATE TABLE bench_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, quantity INTEGER NOT NULL CHECK(quantity > 0))",
        "INSERT INTO bench_products VALUES (1, 1000000000)",
    ] {
        setup.simple_query(sql).await?;
    }
    let run_id = format!(
        "{}-{engine}-transactions-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        std::process::id()
    );
    let dir = output.join(&run_id);
    fs::create_dir_all(&dir)?;
    let manifest = json!({"run_id":run_id,"engine":engine,"profile":"B05-sale","clients":clients,"warmup_seconds":warmup,"measured_seconds":seconds,"rate_ops_s":rate,"mode":if rate.is_some(){"open-loop scheduled; bounded clients"}else{"closed-loop"},"cache_mode":"warm","durability":"synchronous WAL","build":if cfg!(debug_assertions){"dev"}else{"release"},"query_hash":hash(b"stock guard; order; item; commit"),"driver":"tokio-postgres 0.7.15 simple query","conflicts":"reported, not retried","utc_unix_seconds":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()});
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    let start = Instant::now() + Duration::from_millis(100);
    let measured_start = start + Duration::from_secs(warmup);
    let end = measured_start + Duration::from_secs(seconds);
    let mut tasks = Vec::new();
    for worker in 0..clients {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            let client=connect(&engine).await?;let mut samples=Vec::new();let mut seq=0u64;let mut scheduled=start;
            loop{
                if scheduled>=end{break;}tokio::time::sleep_until(scheduled.into()).await;
                let actual=Instant::now();let measured=scheduled>=measured_start;
                if actual>=end&&rate.is_none(){break;}
                let id=((worker as u64+1)<<32)+seq+1;
                let mut error=None;
                if rate.is_some()&&actual>=end{error=Some("overload: missed scheduled deadline".to_owned());}
                else {
                    let result=async{
                        client.simple_query("BEGIN").await?;
                        let result=client.simple_query("UPDATE bench_products SET stock=stock-1 WHERE id=1 AND stock>=1 RETURNING stock").await?;
                        ensure!(result.iter().any(|r|matches!(r,SimpleQueryMessage::Row(_))),"stock exhausted");
                        client.simple_query(&format!("INSERT INTO bench_orders VALUES ({id},1)")).await?;
                        client.simple_query(&format!("INSERT INTO bench_items VALUES ({id},{id},1)")).await?;
                        client.simple_query("COMMIT").await?;Ok::<_,anyhow::Error>(())
                    }.await;
                    if let Err(e)=result{let _=client.simple_query("ROLLBACK").await;error=Some(e.to_string());}
                }
                let finish=Instant::now();
                if measured{samples.push(json!({"query_id":"B05","iteration":seq,"worker":worker,"success":error.is_none(),"error":error,"total_ms":finish.duration_since(if rate.is_some(){scheduled}else{actual}).as_secs_f64()*1000.,"execution_ms":finish.duration_since(actual).as_secs_f64()*1000.,"queue_ms":actual.saturating_duration_since(scheduled).as_secs_f64()*1000.,"rows_returned":0,"retries":0}));}
                seq+=1;
                scheduled=if let Some(rate)=rate{start+Duration::from_secs_f64(seq as f64*clients as f64/rate)}else{finish};
            }
            Ok::<_,anyhow::Error>(samples)
        }));
    }
    let mut log = BufWriter::new(File::create(dir.join("samples.jsonl"))?);
    let mut successes = 0usize;
    let mut errors = 0usize;
    for task in tasks {
        for sample in task.await?? {
            if sample["success"] == true {
                successes += 1;
            } else {
                errors += 1;
            }
            writeln!(log, "{}", serde_json::to_string(&sample)?)?;
        }
    }
    log.flush()?;
    let checks = setup
        .simple_query("SELECT stock FROM bench_products WHERE id=1")
        .await?;
    let stock: i64 = checks
        .iter()
        .find_map(|r| {
            if let SimpleQueryMessage::Row(r) = r {
                r.get(0)
            } else {
                None
            }
        })
        .unwrap()
        .parse()?;
    let rows = setup
        .simple_query("SELECT COUNT(*) FROM bench_orders")
        .await?;
    let orders: i64 = rows
        .iter()
        .find_map(|r| {
            if let SimpleQueryMessage::Row(r) = r {
                r.get(0)
            } else {
                None
            }
        })
        .unwrap()
        .parse()?;
    let rows = setup
        .simple_query("SELECT COUNT(*) FROM bench_items")
        .await?;
    let items: i64 = rows
        .iter()
        .find_map(|r| {
            if let SimpleQueryMessage::Row(r) = r {
                r.get(0)
            } else {
                None
            }
        })
        .unwrap()
        .parse()?;
    let valid = stock >= 0 && orders == items && 1000000000 - stock == orders;
    fs::write(
        dir.join("invariants.json"),
        serde_json::to_vec_pretty(
            &json!({"stock":stock,"orders":orders,"items":items,"valid":valid,"successful_ops":successes,"errors":errors,"throughput_ops_s":successes as f64/seconds as f64,"note":"orders include warmup; measured throughput excludes warmup"}),
        )?,
    )?;
    report(&dir)?;
    ensure!(valid, "transaction invariants failed");
    Ok(())
}
