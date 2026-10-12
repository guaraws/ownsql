use anyhow::{Result, bail};
use rocksdb::{IteratorMode, Options, TransactionDB, TransactionDBOptions, WriteOptions};
use std::{collections::BTreeMap, path::Path};

/// WAL and synchronous commits are mandatory. Catalog and Arrow table images
/// are committed together; this is a small-data prototype, not the final MVCC layout.
pub struct Store {
    db: TransactionDB,
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = TransactionDB::open(&opts, &TransactionDBOptions::default(), path)?;
        if let Some(v) = db.get(b"format")?
            && v != b"ownsql-arrow-v1"
        {
            bail!("unsupported disk format");
        }
        Ok(Self { db })
    }
    pub fn load(&self) -> Result<BTreeMap<String, Vec<u8>>> {
        let mut tables = BTreeMap::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if let Some(name) = key.strip_prefix(b"table/") {
                tables.insert(String::from_utf8(name.to_vec())?, value.to_vec());
            }
        }
        Ok(tables)
    }
    pub fn commit_id(&self) -> Result<u64> {
        match self.db.get(b"commit")? {
            Some(b) => Ok(String::from_utf8(b)?.parse()?),
            None => Ok(0),
        }
    }

    pub fn apply(&self, changes: &BTreeMap<String, Option<Vec<u8>>>, commit: u64) -> Result<()> {
        let mut writes = WriteOptions::default();
        writes.set_sync(true);
        let tx = self.db.transaction_opt(&writes, &Default::default());
        for (name, bytes) in changes {
            match bytes {
                Some(bytes) => tx.put(format!("table/{name}"), bytes)?,
                None => tx.delete(format!("table/{name}"))?,
            }
        }
        tx.put(b"format", b"ownsql-arrow-v1")?;
        tx.put(b"commit", commit.to_string())?;
        tx.commit()?;
        Ok(())
    }
    pub fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        anyhow::ensure!(!path.as_ref().exists(), "backup destination must not exist");
        let backup = Store::open(path)?;
        let changes = self
            .load()?
            .into_iter()
            .map(|(n, b)| (n, Some(b)))
            .collect();
        backup.apply(&changes, self.commit_id()?)?;
        Ok(())
    }
}
