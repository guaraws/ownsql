mod extended;
use anyhow::{Result, ensure};
use clap::Parser;
use executor::{Engine, Output, Session};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tracing::{info, warn};
const MAX_MESSAGE: usize = 16 * 1024 * 1024;
#[derive(Parser)]
#[command(
    version,
    about = "OwnSQL analytical CPU prototype (local connections only)"
)]
struct Config {
    #[arg(long, default_value = "127.0.0.1:55432")]
    listen: SocketAddr,
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 32)]
    max_connections: usize,
    #[arg(long)]
    backup_to: Option<PathBuf>,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::parse();
    ensure!(
        config.listen.ip().is_loopback(),
        "remote access requires TLS/SCRAM, not implemented; bind to loopback"
    );
    ensure!(
        config.max_connections > 0,
        "max-connections must be positive"
    );
    let path = config.data_dir.clone();
    let db = Arc::new(tokio::task::spawn_blocking(move || Engine::open(path)).await??);
    if let Some(path) = config.backup_to {
        db.checkpoint(path).await?;
        return Ok(());
    }
    let listener = TcpListener::bind(config.listen).await?;
    info!(address = %listener.local_addr()?, "OwnSQL 0.2 CPU ready (local trust authentication)");
    let limit = Arc::new(Semaphore::new(config.max_connections));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("shutdown requested"); break; }
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                match limit.clone().try_acquire_owned() {
                    Ok(permit) => { let db = db.clone(); tokio::spawn(async move { let _permit = permit; if let Err(e) = session(socket, db).await { warn!(error = %e, "connection closed"); } }); }
                    Err(_) => { drop(socket); }
                }
            }
        }
    }
    Ok(())
}
async fn frame(socket: &mut TcpStream, kind: u8, bytes: &[u8]) -> Result<()> {
    socket.write_u8(kind).await?;
    socket.write_i32(i32::try_from(bytes.len() + 4)?).await?;
    socket.write_all(bytes).await?;
    Ok(())
}
fn cstr(bytes: &mut Vec<u8>, s: &str) {
    bytes.extend_from_slice(s.as_bytes());
    bytes.push(0);
}
async fn ready(s: &mut TcpStream, state: u8) -> Result<()> {
    frame(s, b'Z', &[state]).await
}
async fn error(s: &mut TcpStream, e: &str) -> Result<()> {
    let code = e
        .get(..5)
        .filter(|c| {
            c.bytes().all(|b| b.is_ascii_alphanumeric()) && e.as_bytes().get(5) == Some(&b':')
        })
        .unwrap_or("XX000");
    let mut b = vec![b'S'];
    cstr(&mut b, "ERROR");
    b.push(b'C');
    cstr(&mut b, code);
    b.push(b'M');
    cstr(&mut b, e);
    b.push(0);
    frame(s, b'E', &b).await
}
async fn packet(s: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let kind = s.read_u8().await?;
    let len = s.read_i32().await?;
    ensure!(
        (4..=MAX_MESSAGE as i32).contains(&len),
        "invalid frontend message length"
    );
    let mut bytes = vec![0; len as usize - 4];
    s.read_exact(&mut bytes).await?;
    Ok((kind, bytes))
}
async fn output(s: &mut TcpStream, out: Output) -> Result<()> {
    if !out.columns.is_empty() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&i16::try_from(out.columns.len())?.to_be_bytes());
        for c in &out.columns {
            cstr(&mut bytes, &c.name);
            bytes.extend_from_slice(&0i32.to_be_bytes());
            bytes.extend_from_slice(&0i16.to_be_bytes());
            bytes.extend_from_slice(&c.oid.to_be_bytes());
            bytes.extend_from_slice(&c.size.to_be_bytes());
            bytes.extend_from_slice(&(-1i32).to_be_bytes());
            bytes.extend_from_slice(&0i16.to_be_bytes());
        }
        frame(s, b'T', &bytes).await?;
        for row in &out.rows {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&i16::try_from(row.len())?.to_be_bytes());
            for value in row {
                if let Some(value) = value {
                    bytes.extend_from_slice(&i32::try_from(value.len())?.to_be_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                } else {
                    bytes.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
            frame(s, b'D', &bytes).await?;
        }
    }
    let mut tag = Vec::new();
    cstr(&mut tag, &out.tag);
    frame(s, b'C', &tag).await
}
async fn copy(s: &mut TcpStream, db: &Engine, session: &mut Session, sql: &str) -> Result<()> {
    let (name, cols) = match db.copy_columns_session(session, sql).await {
        Ok(v) => v,
        Err(e) => {
            session.mark_failed();
            error(s, &e.to_string()).await?;
            return Ok(());
        }
    };
    let mut response = vec![0];
    response.extend_from_slice(&i16::try_from(cols)?.to_be_bytes());
    for _ in 0..cols {
        response.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(s, b'G', &response).await?;
    let mut data = Vec::new();
    let mut too_large = false;
    loop {
        let (kind, bytes) = packet(s).await?;
        match kind {
            b'd' => {
                if data.len() + bytes.len() > MAX_MESSAGE {
                    too_large = true;
                } else if !too_large {
                    data.extend_from_slice(&bytes);
                }
            }
            b'c' if bytes.is_empty() => {
                if too_large {
                    session.mark_failed();
                    error(s, "54000: COPY exceeds 16 MiB; split the load").await?;
                } else {
                    match db.copy_session(session, &name, cols, &data).await {
                        Ok(n) => {
                            let mut tag = Vec::new();
                            cstr(&mut tag, &format!("COPY {n}"));
                            frame(s, b'C', &tag).await?;
                        }
                        Err(e) => {
                            session.mark_failed();
                            error(s, &e.to_string()).await?;
                        }
                    }
                }
                break;
            }
            b'f' => {
                session.mark_failed();
                error(s, "57014: COPY cancelled by client").await?;
                break;
            }
            b'X' => anyhow::bail!("client disconnected during COPY"),
            _ => {
                error(s, "08P01: unexpected COPY message").await?;
                anyhow::bail!("COPY protocol error");
            }
        }
    }
    Ok(())
}
async fn session(mut s: TcpStream, db: Arc<Engine>) -> Result<()> {
    let mut state = Session::default();
    let mut extended = extended::Extended::default();
    s.set_nodelay(true)?;
    loop {
        let len = s.read_i32().await?;
        ensure!((8..=10_000).contains(&len), "invalid startup length");
        let mut bytes = vec![0; len as usize - 4];
        s.read_exact(&mut bytes).await?;
        let code = i32::from_be_bytes(bytes[..4].try_into()?);
        match code {
            80877103 | 80877104 if len == 8 => {
                s.write_all(b"N").await?;
            }
            196608 => {
                ensure!(bytes.last() == Some(&0), "invalid startup parameters");
                break;
            }
            _ => {
                error(&mut s, "0A000: unsupported startup request").await?;
                return Ok(());
            }
        }
    }
    frame(&mut s, b'R', &0i32.to_be_bytes()).await?;
    for (key, value) in [
        ("server_version", "0.2.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("application_name", "ownsql"),
    ] {
        let mut b = Vec::new();
        cstr(&mut b, key);
        cstr(&mut b, value);
        frame(&mut s, b'S', &b).await?;
    }
    ready(&mut s, state.status()).await?;
    let mut extended_error = false;
    loop {
        let (kind, bytes) = match packet(&mut s).await {
            Ok(p) => p,
            Err(e) => return Err(e),
        };
        if extended_error && kind != b'S' && kind != b'X' {
            continue;
        }
        match kind {
            b'X' => break,
            b'Q' => {
                ensure!(
                    bytes.last() == Some(&0) && !bytes[..bytes.len() - 1].contains(&0),
                    "invalid query string"
                );
                let sql = std::str::from_utf8(&bytes[..bytes.len() - 1])?;
                match executor::statements(sql) {
                    Err(e) => {
                        state.mark_failed();
                        error(&mut s, &e.to_string()).await?;
                    }
                    Ok(stmts) if stmts.is_empty() => frame(&mut s, b'I', &[]).await?,
                    Ok(stmts) => {
                        // Reject compound writes: this prototype cannot promise implicit batch transactions.
                        if stmts.len() > 1 {
                            error(&mut s, "0A000: send one statement per query").await?;
                        } else {
                            for stmt in stmts {
                                if stmt.starts_with("COPY ") {
                                    copy(&mut s, &db, &mut state, &stmt).await?;
                                } else {
                                    match db.execute_session(&mut state, &stmt).await {
                                        Ok(out) => output(&mut s, out).await?,
                                        Err(e) => {
                                            error(&mut s, &e.to_string()).await?;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                ready(&mut s, state.status()).await?;
            }
            b'S' if bytes.is_empty() => {
                extended_error = false;
                if state.status() == b'I' {
                    extended.end_transaction();
                }
                ready(&mut s, state.status()).await?;
            }
            b'H' if bytes.is_empty() => {
                s.flush().await?;
            }
            b'P' | b'B' | b'D' | b'E' | b'C' => {
                if let Err(e) = extended.handle(&mut s, &db, &mut state, kind, &bytes).await {
                    state.mark_failed();
                    error(&mut s, &e.to_string()).await?;
                    extended_error = true;
                }
            }
            _ => {
                error(&mut s, "08P01: unsupported frontend message").await?;
                return Ok(());
            }
        }
    }
    Ok(())
}
