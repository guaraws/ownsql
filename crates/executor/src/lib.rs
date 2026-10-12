use anyhow::{Result, bail, ensure};
use datafusion::{
    arrow::{
        array::Array,
        datatypes::{DataType, Schema, SchemaRef},
        ipc::{reader::StreamReader, writer::StreamWriter},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    datasource::MemTable,
    prelude::*,
};
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        AssignmentTarget, ColumnOption, Expr, FromTable, ObjectName, ObjectType, SelectItem,
        Statement, TableConstraint, TableFactor, TableObject, TableWithJoins,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    path::Path,
    sync::Arc,
};
use storage::Store;
use tokio::sync::Mutex;

#[derive(Clone)]
struct Table {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    image: Arc<Vec<u8>>,
}
impl Table {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let mut image = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut image, &schema)?;
            for b in &batches {
                writer.write(b)?;
            }
            writer.finish()?;
        }
        Ok(Self {
            schema,
            batches,
            image: Arc::new(image),
        })
    }
    fn decode(bytes: Vec<u8>) -> Result<Self> {
        let reader = StreamReader::try_new(Cursor::new(&bytes), None)?;
        let schema = reader.schema();
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            schema,
            batches,
            image: Arc::new(bytes),
        })
    }
    fn rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}
type Tables = BTreeMap<String, Arc<Table>>;
#[derive(Default)]
struct State {
    tables: Tables,
    versions: BTreeMap<String, u64>,
    commit: u64,
}
#[derive(Clone)]
struct Transaction {
    tables: Tables,
    versions: BTreeMap<String, u64>,
    changed: BTreeSet<String>,
}
#[derive(Default)]
pub struct Session {
    tx: Option<Transaction>,
    aborted: bool,
    savepoints: Vec<(String, Transaction)>,
    settings: BTreeMap<String, String>,
}
impl Session {
    pub fn status(&self) -> u8 {
        if self.aborted {
            b'E'
        } else if self.tx.is_some() {
            b'T'
        } else {
            b'I'
        }
    }
    pub fn mark_failed(&mut self) {
        if self.tx.is_some() {
            self.aborted = true;
        }
    }
}
pub struct Engine {
    store: Arc<Store>,
    state: Mutex<State>,
}
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub oid: i32,
    pub size: i16,
}
#[derive(Clone, Debug)]
pub struct Output {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
    pub tag: String,
}
impl Output {
    fn command(tag: impl Into<String>) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            tag: tag.into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Constraint {
    NotNull(String),
    Unique(Vec<String>),
    PrimaryKey(Vec<String>),
    Check(String),
}
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn name(n: &ObjectName) -> Result<String> {
    ensure!(
        n.0.len() == 1,
        "0A000: only unqualified table names supported"
    );
    let i = n.0[0]
        .as_ident()
        .ok_or_else(|| anyhow::anyhow!("0A000: identifier required"))?;
    Ok(if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    })
}
fn table_name(t: &TableWithJoins) -> Result<String> {
    ensure!(t.joins.is_empty(), "0A000: DML joins unsupported");
    match &t.relation {
        TableFactor::Table {
            name: n,
            alias: None,
            args: None,
            ..
        } => name(n),
        _ => bail!("0A000: simple DML table required"),
    }
}
pub fn statements(sql: &str) -> Result<Vec<String>> {
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &format!("{sql};"))
        .map_err(|e| anyhow::anyhow!("42601: {e}"))?;
    for stmt in &parsed {
        if let Statement::Copy { values, .. } = stmt {
            ensure!(values.is_empty(), "0A000: inline COPY unsupported");
        }
    }
    Ok(parsed.into_iter().map(|s| s.to_string()).collect())
}
fn parse(sql: &str) -> Result<Statement> {
    let mut s = Parser::parse_sql(&PostgreSqlDialect {}, &format!("{sql};"))
        .map_err(|e| anyhow::anyhow!("42601: {e}"))?;
    ensure!(s.len() == 1, "0A000: one statement required");
    Ok(s.remove(0))
}
pub fn oid(t: &DataType) -> Result<(i32, i16)> {
    Ok(match t {
        DataType::Boolean => (16, 1),
        DataType::Int16 => (21, 2),
        DataType::Int32 => (23, 4),
        DataType::Int64 | DataType::UInt64 => (20, 8),
        DataType::Float32 => (700, 4),
        DataType::Float64 => (701, 8),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => (25, -1),
        DataType::Date32 => (1082, 4),
        DataType::Timestamp(_, None) => (1114, 8),
        DataType::Decimal128(_, _) => (1700, -1),
        _ => bail!("0A000: unsupported result codec {t:?}"),
    })
}
pub fn sql_type(t: &DataType) -> Result<String> {
    Ok(match t {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Timestamp(_, None) => "TIMESTAMP".into(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        _ => bail!("0A000: unsupported storage type"),
    })
}
fn limited_check(e: &Expr) -> bool {
    match e {
        Expr::Identifier(_) | Expr::Value(_) => true,
        Expr::BinaryOp { left, right, .. } => limited_check(left) && limited_check(right),
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => limited_check(expr),
        _ => false,
    }
}
fn constraints(schema: &Schema) -> Result<Vec<Constraint>> {
    Ok(schema
        .metadata()
        .get("ownsql.constraints")
        .map(|s| serde_json::from_str(s))
        .transpose()?
        .unwrap_or_default())
}
fn encode_schema(schema: &Schema, rules: &[Constraint]) -> Result<SchemaRef> {
    let mut metadata = schema.metadata().clone();
    metadata.insert("ownsql.constraints".into(), serde_json::to_string(rules)?);
    Ok(Arc::new(Schema::new_with_metadata(
        schema.fields().clone(),
        metadata,
    )))
}
fn columns(schema: &Schema) -> Result<Vec<Column>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let (oid, size) = oid(f.data_type())?;
            Ok(Column {
                name: f.name().clone(),
                oid,
                size,
            })
        })
        .collect()
}
fn output(schema: &Schema, batches: &[RecordBatch], tag: &str) -> Result<Output> {
    let mut rows = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::new();
            for a in batch.columns() {
                row.push(if a.is_null(r) {
                    None
                } else {
                    let mut text = array_value_to_string(a, r)?;
                    if a.data_type() == &DataType::Boolean {
                        text = if text == "true" { "t" } else { "f" }.into();
                    }
                    if matches!(a.data_type(), DataType::Timestamp(_, None)) {
                        text = text.replace('T', " ");
                    }
                    if a.data_type() == &DataType::UInt64 {
                        ensure!(
                            text.parse::<u64>()? <= i64::MAX as u64,
                            "22003: count exceeds bigint"
                        );
                    }
                    if matches!(a.data_type(), DataType::Float32 | DataType::Float64) {
                        text = match text.as_str() {
                            "inf" => "Infinity".into(),
                            "-inf" => "-Infinity".into(),
                            _ => text,
                        };
                    }
                    Some(text)
                });
            }
            rows.push(row);
        }
    }
    Ok(Output {
        columns: columns(schema)?,
        tag: if tag == "SELECT" {
            format!("SELECT {}", rows.len())
        } else {
            tag.into()
        },
        rows,
    })
}
impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = Arc::new(Store::open(path)?);
        let commit = store.commit_id()?;
        let tables = store
            .load()?
            .into_iter()
            .map(|(n, b)| Ok((n, Arc::new(Table::decode(b)?))))
            .collect::<Result<Tables>>()?;
        let versions = tables.keys().map(|n| (n.clone(), commit)).collect();
        Ok(Self {
            store,
            state: Mutex::new(State {
                tables,
                versions,
                commit,
            }),
        })
    }
    fn context(tables: &Tables) -> Result<SessionContext> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        for (name, t) in tables {
            ctx.register_table(
                name.as_str(),
                Arc::new(MemTable::try_new(
                    t.schema.clone(),
                    vec![t.batches.clone()],
                )?),
            )?;
        }
        Ok(ctx)
    }
    async fn snapshot(&self) -> Transaction {
        let state = self.state.lock().await;
        Transaction {
            tables: state.tables.clone(),
            versions: state.versions.clone(),
            changed: BTreeSet::new(),
        }
    }
    async fn commit(&self, tx: Transaction) -> Result<()> {
        if tx.changed.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        for n in &tx.changed {
            ensure!(
                state.versions.get(n) == tx.versions.get(n),
                "40001: concurrent table update; retry transaction"
            );
        }
        let commit = state
            .commit
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("54000: commit counter exhausted"))?;
        let changes: BTreeMap<String, Option<Vec<u8>>> = tx
            .changed
            .iter()
            .map(|n| {
                (
                    n.clone(),
                    tx.tables.get(n).map(|t| t.image.as_ref().clone()),
                )
            })
            .collect();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.apply(&changes, commit)).await??;
        for n in tx.changed {
            if let Some(t) = tx.tables.get(&n) {
                state.tables.insert(n.clone(), t.clone());
            } else {
                state.tables.remove(&n);
            }
            state.versions.insert(n, commit);
        }
        state.commit = commit;
        Ok(())
    }
    pub async fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let _guard = self.state.lock().await;
        let store = self.store.clone();
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || store.checkpoint(path)).await??;
        Ok(())
    }
    pub async fn execute(&self, sql: &str) -> Result<Output> {
        self.execute_session(&mut Session::default(), sql).await
    }
    pub async fn execute_session(&self, session: &mut Session, sql: &str) -> Result<Output> {
        let result = self.execute_inner(session, sql).await;
        if result.is_err() {
            session.mark_failed();
        }
        result
    }
    async fn execute_inner(&self, s: &mut Session, sql: &str) -> Result<Output> {
        let upper = sql.trim().trim_end_matches(';').to_uppercase();
        if upper == "ROLLBACK" || upper == "ROLLBACK WORK" || upper == "ROLLBACK TRANSACTION" {
            s.tx = None;
            s.aborted = false;
            s.savepoints.clear();
            return Ok(Output::command("ROLLBACK"));
        }
        if upper.starts_with("ROLLBACK TO ") {
            let n = sql.split_whitespace().last().unwrap().trim_end_matches(';');
            let idx = s
                .savepoints
                .iter()
                .rposition(|(name, _)| name == n)
                .ok_or_else(|| anyhow::anyhow!("3B001: savepoint not found"))?;
            s.tx = Some(s.savepoints[idx].1.clone());
            s.savepoints.truncate(idx + 1);
            s.aborted = false;
            return Ok(Output::command("ROLLBACK"));
        }
        if upper == "COMMIT" || upper == "COMMIT WORK" || upper == "COMMIT TRANSACTION" {
            let aborted = s.aborted;
            s.aborted = false;
            s.savepoints.clear();
            if let Some(tx) = s.tx.take()
                && !aborted
            {
                self.commit(tx).await?;
            }
            return Ok(Output::command(if aborted { "ROLLBACK" } else { "COMMIT" }));
        }
        ensure!(!s.aborted, "25P02: transaction aborted; rollback required");
        if [
            "BEGIN",
            "BEGIN WORK",
            "BEGIN TRANSACTION",
            "START TRANSACTION",
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
            "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        ]
        .contains(&upper.as_str())
        {
            ensure!(s.tx.is_none(), "25001: already in transaction");
            s.tx = Some(self.snapshot().await);
            return Ok(Output::command("BEGIN"));
        }
        if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
            bail!("0A000: only snapshot/repeatable read isolation supported");
        }
        if upper.starts_with("SAVEPOINT ") {
            ensure!(s.tx.is_some(), "25P01: no transaction");
            let n = sql
                .split_whitespace()
                .last()
                .unwrap()
                .trim_end_matches(';')
                .to_owned();
            s.savepoints.push((n, s.tx.as_ref().unwrap().clone()));
            return Ok(Output::command("SAVEPOINT"));
        }
        if upper.starts_with("RELEASE ") {
            let n = sql.split_whitespace().last().unwrap().trim_end_matches(';');
            let idx = s
                .savepoints
                .iter()
                .rposition(|(name, _)| name == n)
                .ok_or_else(|| anyhow::anyhow!("3B001: savepoint not found"))?;
            s.savepoints.truncate(idx);
            return Ok(Output::command("RELEASE"));
        }
        if upper.starts_with("SET ") {
            let stmt = parse(sql)?;
            let canonical = stmt.to_string();
            let text = canonical.strip_prefix("SET ").unwrap_or("");
            let (key, value) = text
                .split_once(" = ")
                .or_else(|| text.split_once(" TO "))
                .ok_or_else(|| anyhow::anyhow!("0A000: SET syntax unsupported"))?;
            let key = key.to_lowercase();
            let value = value.trim_matches('\'').to_owned();
            match key.as_str() {
                "client_encoding" => ensure!(
                    value.eq_ignore_ascii_case("UTF8") || value.eq_ignore_ascii_case("UTF-8"),
                    "0A000: only UTF8 supported"
                ),
                "timezone" => ensure!(
                    value.eq_ignore_ascii_case("UTC"),
                    "0A000: only UTC session timezone supported"
                ),
                "search_path" => ensure!(value == "public", "0A000: only public schema supported"),
                "application_name" => {}
                _ => bail!("0A000: SET option unsupported"),
            };
            s.settings.insert(key, value);
            return Ok(Output::command("SET"));
        }
        if upper == "SHOW OWNSQL TABLES" {
            let tx = match &s.tx {
                Some(t) => t.clone(),
                None => self.snapshot().await,
            };
            return Ok(Output {
                columns: vec![Column {
                    name: "table_name".into(),
                    oid: 25,
                    size: -1,
                }],
                rows: tx.tables.keys().map(|n| vec![Some(n.clone())]).collect(),
                tag: "SHOW".into(),
            });
        }
        if upper.starts_with("SHOW OWNSQL COLUMNS ") {
            let n = sql.trim()[20..].trim().trim_matches('"');
            let tx = match &s.tx {
                Some(t) => t.clone(),
                None => self.snapshot().await,
            };
            let t = tx
                .tables
                .get(n)
                .ok_or_else(|| anyhow::anyhow!("42P01: unknown table {n}"))?;
            let rules = constraints(&t.schema)?;
            let names = ["name", "sql_type", "oid", "nullable", "primary_key"];
            let rows=t.schema.fields().iter().map(|f|Ok(vec![Some(f.name().clone()),Some(sql_type(f.data_type())?),Some(oid(f.data_type())?.0.to_string()),Some(if rules.iter().any(|r|matches!(r,Constraint::NotNull(c) if c==f.name())){"f"}else{"t"}.into()),Some(if rules.iter().any(|r|matches!(r,Constraint::PrimaryKey(cols) if cols.contains(f.name()))){"t"}else{"f"}.into())])).collect::<Result<Vec<_>>>()?;
            return Ok(Output {
                columns: names
                    .iter()
                    .map(|n| Column {
                        name: (*n).into(),
                        oid: 25,
                        size: -1,
                    })
                    .collect(),
                rows,
                tag: "SHOW".into(),
            });
        }
        if upper.starts_with("SHOW ") {
            let key = sql.trim()[5..].trim().trim_end_matches(';').to_lowercase();
            let value = s
                .settings
                .get(&key)
                .cloned()
                .or_else(|| match key.as_str() {
                    "transaction_isolation" => Some("repeatable read".into()),
                    "timezone" => Some("UTC".into()),
                    "search_path" => Some("public".into()),
                    "server_version" => Some("0.2.0".into()),
                    _ => None,
                })
                .ok_or_else(|| anyhow::anyhow!("0A000: SHOW option unsupported"))?;
            return Ok(Output {
                columns: vec![Column {
                    name: key,
                    oid: 25,
                    size: -1,
                }],
                rows: vec![vec![Some(value)]],
                tag: "SHOW".into(),
            });
        }
        let mut tx = match &s.tx {
            Some(tx) => tx.clone(),
            None => self.snapshot().await,
        };
        let out = self.run(&mut tx, sql).await?;
        if s.tx.is_some() {
            s.tx = Some(tx);
        } else {
            self.commit(tx).await?;
        }
        Ok(out)
    }
    async fn collect(ctx: &SessionContext, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let df = ctx.sql(sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        Ok((schema, batches))
    }
    async fn read(ctx: &SessionContext, sql: &str) -> Result<Output> {
        let (schema, batches) = Self::collect(ctx, sql).await?;
        output(&schema, &batches, "SELECT")
    }
    async fn materialize(
        ctx: &SessionContext,
        name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<Table>> {
        let (_, batches) = Self::collect(ctx, &format!("SELECT * FROM {}", ident(name))).await?;
        let batches = batches
            .into_iter()
            .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Arc::new(Table::new(schema, batches)?))
    }
    async fn validate(t: &Table, name: &str) -> Result<()> {
        let ctx = Self::context(&BTreeMap::from([(name.into(), Arc::new(t.clone()))]))?;
        for rule in constraints(&t.schema)? {
            let (sql, code) = match rule {
                Constraint::NotNull(c) => (
                    format!(
                        "SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1",
                        ident(name),
                        ident(&c)
                    ),
                    "23502",
                ),
                Constraint::Unique(cols) | Constraint::PrimaryKey(cols) => {
                    let group = cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(",");
                    let nonnull = cols
                        .iter()
                        .map(|c| format!("{} IS NOT NULL", ident(c)))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    (
                        format!(
                            "SELECT 1 FROM {} WHERE {nonnull} GROUP BY {group} HAVING COUNT(*) > 1 LIMIT 1",
                            ident(name)
                        ),
                        "23505",
                    )
                }
                Constraint::Check(e) => (
                    format!("SELECT 1 FROM {} WHERE ({e}) IS FALSE LIMIT 1", ident(name)),
                    "23514",
                ),
            };
            ensure!(
                Self::read(&ctx, &sql).await?.rows.is_empty(),
                "{code}: constraint violation on table {name}"
            );
        }
        Ok(())
    }
    async fn returning(
        t: &Table,
        name: &str,
        items: &Option<Vec<SelectItem>>,
        tag: &str,
    ) -> Result<Output> {
        if let Some(items) = items {
            let ctx = Self::context(&BTreeMap::from([(name.into(), Arc::new(t.clone()))]))?;
            let mut out = Self::read(
                &ctx,
                &format!(
                    "SELECT {} FROM {}",
                    items
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                    ident(name)
                ),
            )
            .await?;
            out.tag = tag.into();
            Ok(out)
        } else {
            Ok(Output::command(tag))
        }
    }
    async fn run(&self, tx: &mut Transaction, sql: &str) -> Result<Output> {
        let stmt = parse(sql)?;
        let ctx = Self::context(&tx.tables)?;
        match stmt {
            Statement::Query(_) => Self::read(&ctx, sql).await,
            Statement::Explain { ref statement, .. }
                if matches!(statement.as_ref(), Statement::Query(_)) =>
            {
                Self::read(&ctx, sql).await
            }
            Statement::CreateTable(mut t) => {
                let n = name(&t.name)?;
                ensure!(!t.columns.is_empty(), "0A000: explicit columns required");
                let mut rules = Vec::new();
                let original = Statement::CreateTable(t.clone()).to_string();
                let expected = format!(
                    "CREATE TABLE {}{} ({})",
                    if t.if_not_exists {
                        "IF NOT EXISTS "
                    } else {
                        ""
                    },
                    t.name,
                    t.columns
                        .iter()
                        .map(ToString::to_string)
                        .chain(t.constraints.iter().map(ToString::to_string))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                ensure!(
                    original == expected,
                    "0A000: unsupported CREATE TABLE option"
                );
                if tx.tables.contains_key(&n) && t.if_not_exists {
                    return Ok(Output::command("CREATE TABLE"));
                }
                for c in &mut t.columns {
                    let ty = c.data_type.to_string().to_uppercase();
                    ensure!(
                        [
                            "SMALLINT",
                            "INT",
                            "INTEGER",
                            "BIGINT",
                            "BOOLEAN",
                            "BOOL",
                            "TEXT",
                            "REAL",
                            "DOUBLE PRECISION",
                            "DATE",
                            "TIMESTAMP"
                        ]
                        .contains(&ty.as_str())
                            || ty.starts_with("NUMERIC(")
                            || ty.starts_with("DECIMAL(")
                            || ty == "TIMESTAMP(6)",
                        "0A000: unsupported type {ty}"
                    );
                    for opt in &c.options {
                        match &opt.option {
                            ColumnOption::Null => {}
                            ColumnOption::NotNull => {
                                rules.push(Constraint::NotNull(c.name.value.clone()))
                            }
                            ColumnOption::Unique {
                                is_primary,
                                characteristics: None,
                            } => {
                                rules.push(if *is_primary {
                                    Constraint::PrimaryKey(vec![c.name.value.clone()])
                                } else {
                                    Constraint::Unique(vec![c.name.value.clone()])
                                });
                                if *is_primary {
                                    rules.push(Constraint::NotNull(c.name.value.clone()));
                                }
                            }
                            ColumnOption::Check(e) if limited_check(e) => {
                                rules.push(Constraint::Check(e.to_string()))
                            }
                            _ => bail!("0A000: unsupported column option"),
                        }
                    }
                    c.options.clear();
                }
                for rule in &t.constraints {
                    match rule {
                        TableConstraint::PrimaryKey {
                            columns,
                            index_type: None,
                            index_options,
                            characteristics: None,
                            ..
                        }
                        | TableConstraint::Unique {
                            columns,
                            index_type: None,
                            index_options,
                            characteristics: None,
                            ..
                        } if index_options.is_empty() => {
                            let cols = columns
                                .iter()
                                .map(|c| {
                                    let text = c.to_string();
                                    ensure!(
                                        !text.contains([' ', '(', ')']),
                                        "0A000: simple constraint columns required"
                                    );
                                    Ok(text.trim_matches('"').to_owned())
                                })
                                .collect::<Result<Vec<_>>>()?;
                            if matches!(rule, TableConstraint::PrimaryKey { .. }) {
                                for c in &cols {
                                    rules.push(Constraint::NotNull(c.clone()));
                                }
                            }
                            rules.push(if matches!(rule, TableConstraint::PrimaryKey { .. }) {
                                Constraint::PrimaryKey(cols)
                            } else {
                                Constraint::Unique(cols)
                            });
                        }
                        TableConstraint::Check { expr, .. } if limited_check(expr) => {
                            rules.push(Constraint::Check(expr.to_string()))
                        }
                        _ => bail!("0A000: unsupported table constraint"),
                    }
                }
                t.constraints.clear();
                Self::collect(&ctx, &Statement::CreateTable(t).to_string()).await?;
                let df = ctx.table(n.as_str()).await?;
                let schema = encode_schema(df.schema().as_arrow(), &rules)?;
                for f in schema.fields() {
                    if let DataType::Decimal128(p, s) = f.data_type() {
                        ensure!(
                            *p <= 18 && *s >= 0 && (*s as u8) <= *p,
                            "0A000: decimal requires precision <=18 and scale 0..precision"
                        );
                    }
                }
                let table = Self::materialize(&ctx, &n, schema).await?;
                Self::validate(&table, &n).await?;
                tx.tables.insert(n.clone(), table);
                tx.changed.insert(n);
                Ok(Output::command("CREATE TABLE"))
            }
            Statement::Insert(mut i) => {
                ensure!(
                    i.on.is_none()
                        && !i.overwrite
                        && !i.ignore
                        && i.or.is_none()
                        && i.table_alias.is_none()
                        && i.partitioned.is_none()
                        && i.assignments.is_empty()
                        && i.source.is_some()
                        && i.after_columns.is_empty()
                        && !i.has_table_keyword
                        && !i.replace_into
                        && i.priority.is_none()
                        && i.insert_alias.is_none()
                        && i.settings.is_none()
                        && i.format_clause.is_none(),
                    "0A000: unsupported INSERT option"
                );
                let n = match &i.table {
                    TableObject::TableName(n) => name(n)?,
                    _ => bail!("0A000: simple INSERT table required"),
                };
                let old = tx
                    .tables
                    .get(&n)
                    .ok_or_else(|| anyhow::anyhow!("42P01: unknown table {n}"))?
                    .clone();
                let returning = i.returning.take();
                Self::collect(&ctx, &Statement::Insert(i).to_string()).await?;
                let table = Self::materialize(&ctx, &n, old.schema.clone()).await?;
                Self::validate(&table, &n).await?;
                let count = table.rows() - old.rows();
                let mut skip = old.rows();
                let mut inserted = Vec::new();
                for b in &table.batches {
                    if skip >= b.num_rows() {
                        skip -= b.num_rows();
                    } else {
                        inserted.push(b.slice(skip, b.num_rows() - skip));
                        skip = 0;
                    }
                }
                let inserted = Table::new(table.schema.clone(), inserted)?;
                let out = Self::returning(&inserted, &n, &returning, &format!("INSERT 0 {count}"))
                    .await?;
                tx.tables.insert(n.clone(), table);
                tx.changed.insert(n);
                Ok(out)
            }
            Statement::Update {
                table,
                assignments,
                selection,
                from: None,
                returning,
                or: None,
                ..
            } => {
                let n = table_name(&table)?;
                let old = tx
                    .tables
                    .get(&n)
                    .ok_or_else(|| anyhow::anyhow!("42P01: unknown table {n}"))?
                    .clone();
                let predicate = selection
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "TRUE".into());
                let mut updates = BTreeMap::new();
                for a in assignments {
                    let col = match a.target {
                        AssignmentTarget::ColumnName(n) => name(&n)?,
                        _ => bail!("0A000: tuple assignments unsupported"),
                    };
                    ensure!(
                        old.schema.index_of(&col).is_ok(),
                        "42703: unknown column {col}"
                    );
                    ensure!(
                        updates.insert(col, a.value.to_string()).is_none(),
                        "42601: duplicate assignment"
                    );
                }
                let changed = old
                    .schema
                    .fields()
                    .iter()
                    .map(|f| {
                        Ok(format!(
                            "CAST({} AS {}) AS {}",
                            updates
                                .get(f.name())
                                .cloned()
                                .unwrap_or_else(|| ident(f.name())),
                            sql_type(f.data_type())?,
                            ident(f.name())
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(",");
                let (_, changed_batches) = Self::collect(
                    &ctx,
                    &format!("SELECT {changed} FROM {} WHERE {predicate}", ident(&n)),
                )
                .await?;
                let changed_batches = changed_batches
                    .into_iter()
                    .map(|b| RecordBatch::try_new(old.schema.clone(), b.columns().to_vec()))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let changed_table = Table::new(old.schema.clone(), changed_batches)?;
                let count = changed_table.rows();
                let (_, mut batches) = Self::collect(
                    &ctx,
                    &format!(
                        "SELECT * FROM {} WHERE ({predicate}) IS NOT TRUE",
                        ident(&n)
                    ),
                )
                .await?;
                batches.extend(changed_table.batches.clone());
                let table = Arc::new(Table::new(
                    old.schema.clone(),
                    batches
                        .into_iter()
                        .map(|b| RecordBatch::try_new(old.schema.clone(), b.columns().to_vec()))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )?);
                Self::validate(&table, &n).await?;
                let out =
                    Self::returning(&changed_table, &n, &returning, &format!("UPDATE {count}"))
                        .await?;
                if count > 0 {
                    tx.tables.insert(n.clone(), table);
                    tx.changed.insert(n);
                }
                Ok(out)
            }
            Statement::Delete(d) => {
                ensure!(
                    d.tables.is_empty()
                        && d.using.is_none()
                        && d.order_by.is_empty()
                        && d.limit.is_none(),
                    "0A000: unsupported DELETE option"
                );
                let from = match d.from {
                    FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
                };
                ensure!(from.len() == 1, "0A000: simple DELETE required");
                let n = table_name(&from[0])?;
                let old = tx
                    .tables
                    .get(&n)
                    .ok_or_else(|| anyhow::anyhow!("42P01: unknown table {n}"))?
                    .clone();
                let predicate = d
                    .selection
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "TRUE".into());
                let (_, removed) = Self::collect(
                    &ctx,
                    &format!("SELECT * FROM {} WHERE {predicate}", ident(&n)),
                )
                .await?;
                let removed = Table::new(old.schema.clone(), removed)?;
                let count = removed.rows();
                let (_, batches) = Self::collect(
                    &ctx,
                    &format!(
                        "SELECT * FROM {} WHERE ({predicate}) IS NOT TRUE",
                        ident(&n)
                    ),
                )
                .await?;
                let table = Arc::new(Table::new(old.schema.clone(), batches)?);
                let out =
                    Self::returning(&removed, &n, &d.returning, &format!("DELETE {count}")).await?;
                if count > 0 {
                    tx.tables.insert(n.clone(), table);
                    tx.changed.insert(n);
                }
                Ok(out)
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                if_exists,
                ..
            } if names.len() == 1 => {
                let n = name(&names[0])?;
                ensure!(
                    tx.tables.contains_key(&n) || if_exists,
                    "42P01: unknown table {n}"
                );
                tx.tables.remove(&n);
                tx.changed.insert(n);
                Ok(Output::command("DROP TABLE"))
            }
            _ => bail!("0A000: SQL statement unsupported"),
        }
    }
    pub async fn describe(&self, s: &Session, sql: &str) -> Result<Vec<Column>> {
        let tx = match &s.tx {
            Some(t) => t.clone(),
            None => self.snapshot().await,
        };
        let ctx = Self::context(&tx.tables)?;
        match parse(sql)? {
            Statement::Query(_) => {
                let df = ctx.sql(sql).await?;
                columns(df.schema().as_arrow())
            }
            Statement::Insert(i) => {
                if i.returning.is_none() {
                    return Ok(vec![]);
                }
                let n = match &i.table {
                    TableObject::TableName(n) => name(n)?,
                    _ => bail!("0A000: unsupported table"),
                };
                let df = ctx
                    .sql(&format!(
                        "SELECT {} FROM {}",
                        i.returning
                            .unwrap()
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                        ident(&n)
                    ))
                    .await?;
                columns(df.schema().as_arrow())
            }
            Statement::Update {
                table,
                returning: Some(items),
                ..
            } => {
                let n = table_name(&table)?;
                let df = ctx
                    .sql(&format!(
                        "SELECT {} FROM {}",
                        items
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                        ident(&n)
                    ))
                    .await?;
                columns(df.schema().as_arrow())
            }
            Statement::Delete(d) if d.returning.is_some() => {
                let from = match d.from {
                    FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
                };
                ensure!(from.len() == 1, "0A000: simple DELETE required");
                let n = table_name(&from[0])?;
                let df = ctx
                    .sql(&format!(
                        "SELECT {} FROM {}",
                        d.returning
                            .unwrap()
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                        ident(&n)
                    ))
                    .await?;
                columns(df.schema().as_arrow())
            }
            _ => Ok(vec![]),
        }
    }
    pub async fn parameter_types(
        &self,
        s: &Session,
        sql: &str,
    ) -> Result<BTreeMap<String, Option<DataType>>> {
        let tx = match &s.tx {
            Some(t) => t.clone(),
            None => self.snapshot().await,
        };
        let stmt = parse(sql)?;
        let mut types = BTreeMap::new();
        match &stmt {
            Statement::Query(q) => {
                if let Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) =
                    &q.limit_clause
                {
                    if let Some(e) = limit
                        && let Some(p) = placeholder(e)
                    {
                        types.insert(p.into(), Some(DataType::Int64));
                    }
                    if let Some(e) = offset
                        && let Some(p) = placeholder(&e.value)
                    {
                        types.insert(p.into(), Some(DataType::Int64));
                    }
                }

                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(e)
                            | SelectItem::ExprWithAlias { expr: e, .. } => {
                                infer_expr(e, &tx.tables, &mut types)?
                            }
                            _ => {}
                        }
                    }
                    if let Some(e) = &select.selection {
                        infer_expr(e, &tx.tables, &mut types)?;
                    }
                    if let Some(e) = &select.having {
                        infer_expr(e, &tx.tables, &mut types)?;
                    }
                }
            }
            Statement::Update {
                assignments,
                selection,
                ..
            } => {
                for a in assignments {
                    infer_expr(&a.value, &tx.tables, &mut types)?;
                }
                if let Some(e) = selection {
                    infer_expr(e, &tx.tables, &mut types)?;
                }
            }
            Statement::Delete(d) => {
                if let Some(e) = &d.selection {
                    infer_expr(e, &tx.tables, &mut types)?;
                }
            }
            Statement::Insert(i) => {
                if let TableObject::TableName(n) = &i.table
                    && let Some(table) = tx.tables.get(&name(n)?)
                    && let Some(q) = &i.source
                    && let sqlparser::ast::SetExpr::Values(v) = q.body.as_ref()
                {
                    for row in &v.rows {
                        for (index, e) in row.iter().enumerate() {
                            infer_expr(e, &tx.tables, &mut types)?;
                            if let Some(p) = placeholder(e) {
                                let f = if i.columns.is_empty() {
                                    table.schema.fields().get(index).cloned()
                                } else {
                                    i.columns
                                        .get(index)
                                        .and_then(|c| table.schema.field_with_name(&c.value).ok())
                                        .map(|f| Arc::new(f.clone()))
                                };
                                if let Some(f) = f {
                                    types.insert(p.into(), Some(f.data_type().clone()));
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(types)
    }
    pub async fn copy_columns(&self, sql: &str) -> Result<(String, usize)> {
        self.copy_columns_session(&Session::default(), sql).await
    }
    pub async fn copy_columns_session(&self, s: &Session, sql: &str) -> Result<(String, usize)> {
        let stmt = parse(sql)?;
        ensure!(
            matches!(&stmt,Statement::Copy{values,..} if values.is_empty()),
            "0A000: inline COPY unsupported"
        );
        let canonical = stmt.to_string();
        let n = canonical
            .strip_prefix("COPY ")
            .and_then(|s| s.strip_suffix(" FROM STDIN"))
            .ok_or_else(|| anyhow::anyhow!("0A000: only COPY table FROM STDIN supported"))?;
        ensure!(
            !n.contains([' ', '(', '.', ';']),
            "0A000: simple COPY table required"
        );
        let tx = match &s.tx {
            Some(t) => t.clone(),
            None => self.snapshot().await,
        };
        Ok((
            n.into(),
            tx.tables
                .get(n)
                .ok_or_else(|| anyhow::anyhow!("42P01: unknown COPY table"))?
                .schema
                .fields()
                .len(),
        ))
    }
    pub async fn copy(&self, n: &str, cols: usize, bytes: &[u8]) -> Result<usize> {
        self.copy_session(&mut Session::default(), n, cols, bytes)
            .await
    }
    pub async fn copy_session(
        &self,
        s: &mut Session,
        n: &str,
        cols: usize,
        bytes: &[u8],
    ) -> Result<usize> {
        let text = std::str::from_utf8(bytes)?;
        let mut rows = Vec::new();
        for line in text.split_terminator('\n') {
            let values = line
                .trim_end_matches('\r')
                .split('\t')
                .map(copy_value)
                .collect::<Result<Vec<_>>>()?;
            ensure!(values.len() == cols, "22P04: COPY column count mismatch");
            rows.push(format!("({})", values.join(",")));
        }
        if !rows.is_empty() {
            self.execute_session(s, &format!("INSERT INTO {n} VALUES {}", rows.join(",")))
                .await?;
        }
        Ok(rows.len())
    }
}
fn copy_value(v: &str) -> Result<String> {
    if v == "\\N" {
        return Ok("NULL".into());
    }
    let mut chars = v.chars();
    let mut out = String::new();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        out.push(match chars.next() {
            Some('t') => '\t',
            Some('n') => '\n',
            Some('r') => '\r',
            Some('\\') => '\\',
            _ => bail!("0A000: unsupported COPY escape"),
        });
    }
    Ok(format!("'{}'", out.replace('\'', "''")))
}

fn placeholder(e: &Expr) -> Option<&str> {
    match e {
        Expr::Value(v) => {
            if let sqlparser::ast::Value::Placeholder(p) = &v.value {
                Some(p)
            } else {
                None
            }
        }
        _ => None,
    }
}
fn cast_type(s: &str) -> Option<DataType> {
    let upper = s.to_uppercase();
    if (upper.starts_with("NUMERIC(") || upper.starts_with("DECIMAL(")) && upper.ends_with(')') {
        let body = upper.split_once('(')?.1.trim_end_matches(')');
        let (p, scale) = body.split_once(',')?;
        return Some(DataType::Decimal128(
            p.trim().parse().ok()?,
            scale.trim().parse().ok()?,
        ));
    }
    Some(match s.to_uppercase().as_str() {
        "BIGINT" => DataType::Int64,
        "INT" | "INTEGER" => DataType::Int32,
        "SMALLINT" => DataType::Int16,
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "TEXT" | "VARCHAR" => DataType::Utf8,
        "REAL" => DataType::Float32,
        "DOUBLE PRECISION" => DataType::Float64,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        _ => return None,
    })
}
fn expr_column(e: &Expr, tables: &Tables) -> Option<DataType> {
    let n = match e {
        Expr::Identifier(i) => &i.value,
        Expr::CompoundIdentifier(i) => &i.last()?.value,
        _ => return None,
    };
    let mut t = None;
    for table in tables.values() {
        if let Ok(f) = table.schema.field_with_name(n) {
            if t.as_ref().is_some_and(|old| old != f.data_type()) {
                return None;
            }
            t = Some(f.data_type().clone());
        }
    }
    t
}
fn infer_expr(
    e: &Expr,
    tables: &Tables,
    out: &mut BTreeMap<String, Option<DataType>>,
) -> Result<()> {
    match e {
        Expr::Cast {
            expr, data_type, ..
        } => {
            if let Some(p) = placeholder(expr)
                && let Some(t) = cast_type(&data_type.to_string())
            {
                out.insert(p.into(), Some(t));
            }
            infer_expr(expr, tables, out)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            if let (Some(p), Some(t)) = (placeholder(right), expr_column(left, tables)) {
                out.insert(p.into(), Some(t));
            }
            if let (Some(p), Some(t)) = (placeholder(left), expr_column(right, tables)) {
                out.insert(p.into(), Some(t));
            }
            infer_expr(left, tables, out)?;
            infer_expr(right, tables, out)?;
        }
        Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } | Expr::IsNull(e) | Expr::IsNotNull(e) => {
            infer_expr(e, tables, out)?
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn transaction_conflict_rollback_constraints_backup() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = Engine::open(dir.path().join("db"))?;
        db.execute("CREATE TABLE products (id BIGINT PRIMARY KEY, stock INTEGER NOT NULL CHECK (stock >= 0), sku TEXT UNIQUE)").await?;
        db.execute("INSERT INTO products VALUES (1, 10, 'one'), (2, 20, NULL), (3, 30, NULL)")
            .await?;
        for sql in [
            "INSERT INTO products VALUES (1, 1, 'duplicate')",
            "INSERT INTO products VALUES (4, NULL, 'null')",
            "INSERT INTO products VALUES (4, -1, 'bad')",
            "INSERT INTO products VALUES (4, 1, 'one')",
        ] {
            assert!(db.execute(sql).await.is_err(), "{sql}");
        }
        let mut a = Session::default();
        let mut b = Session::default();
        db.execute_session(&mut a, "BEGIN").await?;
        db.execute_session(&mut b, "BEGIN").await?;
        assert_eq!(
            db.execute_session(
                &mut a,
                "UPDATE products SET stock=stock-2 WHERE id=1 AND stock>=2 RETURNING stock"
            )
            .await?
            .rows,
            vec![vec![Some("8".into())]]
        );
        assert_eq!(
            db.execute_session(&mut b, "SELECT stock FROM products WHERE id=1")
                .await?
                .rows,
            vec![vec![Some("10".into())]]
        );
        db.execute_session(&mut a, "COMMIT").await?;
        db.execute_session(&mut b, "UPDATE products SET stock=stock-3 WHERE id=1")
            .await?;
        assert!(
            db.execute_session(&mut b, "COMMIT")
                .await
                .unwrap_err()
                .to_string()
                .starts_with("40001:")
        );
        assert_eq!(
            db.execute("SELECT stock FROM products WHERE id=1")
                .await?
                .rows,
            vec![vec![Some("8".into())]]
        );
        db.execute_session(&mut a, "BEGIN").await?;
        db.execute_session(&mut a, "SAVEPOINT before_delete")
            .await?;
        db.execute_session(&mut a, "DELETE FROM products WHERE id=1 RETURNING id")
            .await?;
        db.execute_session(&mut a, "ROLLBACK TO SAVEPOINT before_delete")
            .await?;
        assert!(
            db.execute_session(&mut a, "UPDATE products SET stock=-1 WHERE id=1")
                .await
                .is_err()
        );
        assert_eq!(a.status(), b'E');
        assert!(
            db.execute_session(&mut a, "SELECT 1")
                .await
                .unwrap_err()
                .to_string()
                .starts_with("25P02:")
        );
        db.execute_session(&mut a, "ROLLBACK").await?;
        assert_eq!(a.status(), b'I');
        db.checkpoint(dir.path().join("backup")).await?;
        let restored = Engine::open(dir.path().join("backup"))?;
        assert_eq!(
            db.execute("SELECT * FROM products ORDER BY id").await?.rows,
            restored
                .execute("SELECT * FROM products ORDER BY id")
                .await?
                .rows
        );
        assert!(
            restored
                .execute("INSERT INTO products VALUES (1,1,'again')")
                .await
                .is_err()
        );
        Ok(())
    }
    #[tokio::test]
    async fn numeric_and_insert_returning() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = Engine::open(dir.path())?;
        db.execute("CREATE TABLE money (id BIGINT PRIMARY KEY, amount NUMERIC(18,2))")
            .await?;
        assert_eq!(
            db.execute("INSERT INTO money VALUES (1, 12.34), (2, 0.01) RETURNING id")
                .await?
                .rows,
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            db.execute("SELECT SUM(amount) FROM money").await?.rows,
            vec![vec![Some("12.35".into())]]
        );
        Ok(())
    }
}
