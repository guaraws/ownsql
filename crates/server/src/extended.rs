use super::{cstr, frame};
use anyhow::{Result, bail, ensure};
use chrono::{NaiveDate, NaiveDateTime};
use executor::{Column, Engine, Output, Session};
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
};
use std::collections::BTreeMap;
use tokio::net::TcpStream;
#[derive(Default)]
pub struct Extended {
    statements: BTreeMap<String, Prepared>,
    portals: BTreeMap<String, Portal>,
}
struct Prepared {
    sql: String,
    types: Vec<i32>,
    columns: Vec<Column>,
    casts: Vec<String>,
}
struct Portal {
    sql: String,
    columns: Vec<Column>,
    formats: Vec<i16>,
    result: Option<Output>,
    offset: usize,
}
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(self.pos + n <= self.bytes.len(), "08P01: truncated message");
        let b = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(b)
    }
    fn string(&mut self) -> Result<String> {
        let end = self.bytes[self.pos..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| anyhow::anyhow!("08P01: missing string terminator"))?;
        let text = std::str::from_utf8(self.take(end)?)?.to_owned();
        self.take(1)?;
        Ok(text)
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into()?))
    }
    fn count(&mut self) -> Result<usize> {
        let n = self.i16()?;
        ensure!(n >= 0, "08P01: negative count");
        Ok(n as usize)
    }
    fn done(&self) -> Result<()> {
        ensure!(
            self.pos == self.bytes.len(),
            "08P01: unexpected message suffix"
        );
        Ok(())
    }
}
fn typename(oid: i32) -> Result<&'static str> {
    Ok(match oid {
        16 => "BOOLEAN",
        20 => "BIGINT",
        21 => "SMALLINT",
        23 => "INTEGER",
        25 | 1043 | 0 => "TEXT",
        700 => "REAL",
        701 => "DOUBLE PRECISION",
        1082 => "DATE",
        1114 => "TIMESTAMP",
        1700 => "NUMERIC(18,6)",
        _ => bail!("0A000: unsupported parameter OID {oid}"),
    })
}
fn parameters(sql: &str, values: &[String]) -> Result<String> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize()?;
    tokens
        .into_iter()
        .map(|t| match t {
            Token::Placeholder(p) => {
                let n: usize = p
                    .strip_prefix('$')
                    .ok_or_else(|| anyhow::anyhow!("42601: unsupported placeholder"))?
                    .parse()?;
                ensure!(n > 0 && n <= values.len(), "08P01: invalid parameter index");
                Ok(values[n - 1].clone())
            }
            _ => Ok(t.to_string()),
        })
        .collect::<Result<Vec<_>>>()
        .map(|s| s.join(" "))
}
fn epoch() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}
fn binary_text(oid: i32, b: &[u8]) -> Result<String> {
    Ok(match oid {
        16 => {
            ensure!(
                b.len() == 1 && (b[0] == 0 || b[0] == 1),
                "22P03: invalid boolean"
            );
            if b[0] == 1 { "true" } else { "false" }.into()
        }
        21 => i16::from_be_bytes(b.try_into()?).to_string(),
        23 => i32::from_be_bytes(b.try_into()?).to_string(),
        20 => i64::from_be_bytes(b.try_into()?).to_string(),
        700 => f32::from_be_bytes(b.try_into()?).to_string(),
        701 => f64::from_be_bytes(b.try_into()?).to_string(),
        1082 => epoch()
            .date()
            .checked_add_signed(chrono::Duration::days(
                i32::from_be_bytes(b.try_into()?) as i64
            ))
            .ok_or_else(|| anyhow::anyhow!("22008: date out of range"))?
            .to_string(),
        1114 => epoch()
            .checked_add_signed(chrono::Duration::microseconds(i64::from_be_bytes(
                b.try_into()?,
            )))
            .ok_or_else(|| anyhow::anyhow!("22008: timestamp out of range"))?
            .to_string(),
        25 | 1043 => std::str::from_utf8(b)?.into(),
        1700 => numeric_decode(b)?,
        _ => bail!("0A000: unsupported binary parameter"),
    })
}
fn numeric_decode(b: &[u8]) -> Result<String> {
    let mut r = Reader::new(b);
    let n = r.count()?;
    let weight = r.i16()?;
    let sign = r.i16()?;
    let scale = r.count()?;
    ensure!(
        n <= 6 && scale <= 18 && (sign == 0 || sign == 0x4000),
        "0A000: numeric outside supported range"
    );
    let mut digits = Vec::new();
    for _ in 0..n {
        let d = r.i16()?;
        ensure!((0..10000).contains(&d), "22P03: invalid numeric digit");
        digits.push(d);
    }
    r.done()?;
    let at = |pos: i32| -> i16 {
        let i = weight as i32 - pos;
        if i >= 0 {
            digits.get(i as usize).copied().unwrap_or(0)
        } else {
            0
        }
    };
    let mut text = String::new();
    if sign == 0x4000 {
        text.push('-');
    }
    if weight < 0 {
        text.push('0');
    } else {
        for pos in (0..=weight as i32).rev() {
            let d = at(pos);
            if pos == weight as i32 {
                text.push_str(&d.to_string());
            } else {
                text.push_str(&format!("{d:04}"));
            }
        }
    }
    if scale > 0 {
        let mut fraction = String::new();
        for pos in 1..=scale.div_ceil(4) {
            fraction.push_str(&format!("{:04}", at(-(pos as i32))));
        }
        fraction.truncate(scale);
        text.push('.');
        text.push_str(&fraction);
    }
    Ok(text)
}
fn numeric_encode(text: &str) -> Result<Vec<u8>> {
    let negative = text.starts_with('-');
    let text = text.trim_start_matches('-');
    let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        integer
            .chars()
            .chain(fraction.chars())
            .all(|c| c.is_ascii_digit()),
        "0A000: nonfinite numeric unsupported"
    );
    let left = (4 - integer.len() % 4) % 4;
    let right = (4 - fraction.len() % 4) % 4;
    let joined = format!(
        "{}{}{}{}",
        "0".repeat(left),
        integer,
        fraction,
        "0".repeat(right)
    );
    let mut digits = joined
        .as_bytes()
        .chunks(4)
        .map(|c| std::str::from_utf8(c).unwrap().parse::<i16>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut weight = ((integer.len() + left) / 4) as i16 - 1;
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight -= 1;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    let mut out = Vec::new();
    for n in [
        digits.len() as i16,
        weight,
        if negative { 0x4000 } else { 0 },
        fraction.len() as i16,
    ] {
        out.extend_from_slice(&n.to_be_bytes());
    }
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    Ok(out)
}
fn binary_value(oid: i32, text: &str) -> Result<Vec<u8>> {
    Ok(match oid {
        16 => vec![u8::from(text == "t")],
        21 => text.parse::<i16>()?.to_be_bytes().to_vec(),
        23 => text.parse::<i32>()?.to_be_bytes().to_vec(),
        20 => text.parse::<i64>()?.to_be_bytes().to_vec(),
        700 => text.parse::<f32>()?.to_be_bytes().to_vec(),
        701 => text.parse::<f64>()?.to_be_bytes().to_vec(),
        25 | 1043 => text.as_bytes().to_vec(),
        1082 => i32::try_from(
            NaiveDate::parse_from_str(text, "%Y-%m-%d")?
                .signed_duration_since(epoch().date())
                .num_days(),
        )?
        .to_be_bytes()
        .to_vec(),
        1114 => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")?
            .signed_duration_since(epoch())
            .num_microseconds()
            .ok_or_else(|| anyhow::anyhow!("22008: timestamp overflow"))?
            .to_be_bytes()
            .to_vec(),
        1700 => numeric_encode(text)?,
        _ => bail!("0A000: unsupported binary result"),
    })
}
fn format_for(formats: &[i16], i: usize) -> i16 {
    if formats.len() == 1 {
        formats[0]
    } else {
        formats.get(i).copied().unwrap_or(0)
    }
}
pub async fn description(s: &mut TcpStream, columns: &[Column], formats: &[i16]) -> Result<()> {
    if columns.is_empty() {
        return frame(s, b'n', &[]).await;
    }
    let mut b = Vec::new();
    b.extend_from_slice(&i16::try_from(columns.len())?.to_be_bytes());
    for (i, c) in columns.iter().enumerate() {
        cstr(&mut b, &c.name);
        b.extend_from_slice(&0i32.to_be_bytes());
        b.extend_from_slice(&0i16.to_be_bytes());
        b.extend_from_slice(&c.oid.to_be_bytes());
        b.extend_from_slice(&c.size.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&format_for(formats, i).to_be_bytes());
    }
    frame(s, b'T', &b).await
}
impl Extended {
    pub fn end_transaction(&mut self) {
        self.portals.clear();
    }
    pub async fn handle(
        &mut self,
        s: &mut TcpStream,
        db: &Engine,
        session: &mut Session,
        kind: u8,
        bytes: &[u8],
    ) -> Result<()> {
        let mut r = Reader::new(bytes);
        match kind {
            b'P' => {
                let name = r.string()?;
                let sql = r.string()?;
                let count = r.count()?;
                let mut types = Vec::new();
                for _ in 0..count {
                    types.push(r.i32()?);
                }
                r.done()?;
                ensure!(
                    executor::statements(&sql)?.len() == 1,
                    "42601: prepared statement must contain one instruction"
                );
                let tokens = Tokenizer::new(&PostgreSqlDialect {}, &sql).tokenize()?;
                let highest = tokens
                    .iter()
                    .filter_map(|t| {
                        if let Token::Placeholder(p) = t {
                            p.trim_start_matches('$').parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .max()
                    .unwrap_or(0);
                ensure!(highest <= 1024, "54000: too many parameters");
                types.resize(highest.max(types.len()), 0);
                let inferred = db.parameter_types(session, &sql).await.unwrap_or_default();
                for (i, t) in types.iter_mut().enumerate() {
                    if *t == 0 {
                        *t = inferred
                            .get(&format!("${}", i + 1))
                            .and_then(|t| t.as_ref())
                            .and_then(|t| executor::oid(t).ok())
                            .map(|o| o.0)
                            .unwrap_or(25);
                    }
                }
                let casts=types.iter().enumerate().map(|(i,t)| {
                    if let Some(Some(ty))=inferred.get(&format!("${}",i+1)) { return executor::sql_type(ty); }
                    ensure!(*t!=1700,"0A000: numeric parameter requires explicit precision/scale or column context");
                    Ok(typename(*t)?.to_owned())
                }).collect::<Result<Vec<_>>>()?;
                let values = casts
                    .iter()
                    .enumerate()
                    .map(|(i, t)| format!("CAST(${} AS {t})", i + 1))
                    .collect::<Vec<_>>();
                let substituted = parameters(&sql, &values)?;
                let cols = db.describe(session, &substituted).await?;
                ensure!(
                    name.is_empty() || !self.statements.contains_key(&name),
                    "42P05: prepared statement already exists"
                );
                ensure!(self.statements.len() < 1024, "54000: statement limit");
                self.statements.insert(
                    name,
                    Prepared {
                        sql,
                        types,
                        columns: cols,
                        casts,
                    },
                );
                frame(s, b'1', &[]).await?;
            }
            b'B' => {
                let name = r.string()?;
                let statement = r.string()?;
                let p = self
                    .statements
                    .get(&statement)
                    .ok_or_else(|| anyhow::anyhow!("26000: unknown prepared statement"))?;
                let count = r.count()?;
                let mut formats = Vec::new();
                for _ in 0..count {
                    formats.push(r.i16()?);
                }
                let count = r.count()?;
                ensure!(count == p.types.len(), "08P01: parameter count mismatch");
                ensure!(
                    formats.is_empty() || formats.len() == 1 || formats.len() == count,
                    "08P01: parameter format count mismatch"
                );
                ensure!(
                    formats.iter().all(|f| *f == 0 || *f == 1),
                    "08P01: invalid parameter format"
                );
                let mut values = Vec::new();
                for i in 0..count {
                    let len = r.i32()?;
                    ensure!(len >= -1, "08P01: invalid parameter length");
                    let value = if len == -1 {
                        "NULL".into()
                    } else {
                        let bytes = r.take(len as usize)?;
                        let text = if format_for(&formats, i) == 1 {
                            binary_text(p.types[i], bytes)?
                        } else {
                            std::str::from_utf8(bytes)?.into()
                        };
                        format!("'{}'", text.replace('\'', "''"))
                    };
                    values.push(format!("CAST({value} AS {})", p.casts[i]));
                }
                let count = r.count()?;
                let mut formats = Vec::new();
                for _ in 0..count {
                    formats.push(r.i16()?);
                }
                r.done()?;
                ensure!(
                    formats.iter().all(|f| *f == 0 || *f == 1),
                    "08P01: invalid result format"
                );
                ensure!(
                    formats.is_empty() || formats.len() == 1 || formats.len() == p.columns.len(),
                    "08P01: result format count mismatch"
                );
                ensure!(
                    name.is_empty() || !self.portals.contains_key(&name),
                    "42P03: portal already exists"
                );
                ensure!(self.portals.len() < 1024, "54000: portal limit");
                self.portals.insert(
                    name,
                    Portal {
                        sql: parameters(&p.sql, &values)?,
                        columns: p.columns.clone(),
                        formats,
                        result: None,
                        offset: 0,
                    },
                );
                frame(s, b'2', &[]).await?;
            }
            b'D' => {
                let target = r.take(1)?[0];
                let name = r.string()?;
                r.done()?;
                match target {
                    b'S' => {
                        let p = self
                            .statements
                            .get(&name)
                            .ok_or_else(|| anyhow::anyhow!("26000: unknown prepared statement"))?;
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&i16::try_from(p.types.len())?.to_be_bytes());
                        for t in &p.types {
                            bytes.extend_from_slice(&t.to_be_bytes());
                        }
                        frame(s, b't', &bytes).await?;
                        description(s, &p.columns, &[]).await?;
                    }
                    b'P' => {
                        let p = self
                            .portals
                            .get(&name)
                            .ok_or_else(|| anyhow::anyhow!("34000: unknown portal"))?;
                        description(s, &p.columns, &p.formats).await?;
                    }
                    _ => bail!("08P01: invalid Describe target"),
                }
            }
            b'E' => {
                let name = r.string()?;
                let limit = r.i32()?;
                r.done()?;
                ensure!(limit >= 0, "08P01: negative portal limit");
                let p = self
                    .portals
                    .get_mut(&name)
                    .ok_or_else(|| anyhow::anyhow!("34000: unknown portal"))?;
                if p.result.is_none() {
                    p.result = Some(db.execute_session(session, &p.sql).await?);
                }
                let out = p.result.as_ref().unwrap();
                let end = if limit == 0 {
                    out.rows.len()
                } else {
                    out.rows.len().min(p.offset + limit as usize)
                };
                for row in &out.rows[p.offset..end] {
                    let mut b = Vec::new();
                    b.extend_from_slice(&i16::try_from(row.len())?.to_be_bytes());
                    for (i, value) in row.iter().enumerate() {
                        if let Some(text) = value {
                            let value = if format_for(&p.formats, i) == 1 {
                                binary_value(out.columns[i].oid, text)?
                            } else {
                                text.as_bytes().to_vec()
                            };
                            b.extend_from_slice(&i32::try_from(value.len())?.to_be_bytes());
                            b.extend_from_slice(&value);
                        } else {
                            b.extend_from_slice(&(-1i32).to_be_bytes());
                        }
                    }
                    frame(s, b'D', &b).await?;
                }
                p.offset = end;
                if end < out.rows.len() {
                    frame(s, b's', &[]).await?;
                } else {
                    let mut b = Vec::new();
                    cstr(&mut b, &out.tag);
                    frame(s, b'C', &b).await?;
                }
            }
            b'C' => {
                let target = r.take(1)?[0];
                let name = r.string()?;
                r.done()?;
                match target {
                    b'S' => {
                        self.statements.remove(&name);
                    }
                    b'P' => {
                        self.portals.remove(&name);
                    }
                    _ => bail!("08P01: invalid Close target"),
                };
                frame(s, b'3', &[]).await?;
            }
            _ => bail!("08P01: unknown extended message"),
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_codec() {
        for n in ["0", "12.34", "-0.0012", "123456789.123456"] {
            assert_eq!(numeric_decode(&numeric_encode(n).unwrap()).unwrap(), n);
        }
    }
    #[test]
    fn substitutions_preserve_literals() {
        assert_eq!(
            parameters("SELECT '$1', $1", &["CAST('2' AS BIGINT)".into()]).unwrap(),
            "SELECT   '$1' ,   CAST('2' AS BIGINT)"
        );
    }
}
