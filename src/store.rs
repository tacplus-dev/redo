//! The logs on disk: one per trust domain and tenant, a compacted snapshot plus
//! the tail behind it.
//!
//! `<root>/<trust domain>/<tid>/` holds:
//! - `tail`: the operations since the snapshot, in seq order, one frame each.
//!   An append is on disk (fsync) before it returns, which is the LOCAL ack.
//! - `snapshot`: the live entries at the last seq the snapshot folds in. Its
//!   first frame is the header (that seq, no changes); every other frame is one
//!   PUT at the same seq. Replaced whole (write, fsync, rename), never edited.
//! - `shipped`: the last seq the second site acked (this site's own logs only).
//! - `deleted`: the tenant was deleted; its log is gone and it is refused for
//!   good (a tid is never reused). `deletion-unshipped` says the second site has
//!   not acked the deletion yet.
//!
//! A frame is `[len u32 LE][crc32 u32 LE][a ShippedOperation, protobuf]`. A frame
//! cut short at the end of the tail, or whose CRC fails there, is an append that
//! never returned (so was never acked): it is cut off on open. A bad frame
//! anywhere else is corruption, and that tenant's log fails closed.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use prost::Message;
use redo_model::types::services::redo::redo::ShippedOperation;
use redo_model::types::sys::redolog::{redo_key, RedoChange, RedoOp, RedoReplayEntry};

/// The log of one tenant of one trust domain.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogKey {
    pub trust_domain: String,
    pub tid: String,
}

impl LogKey {
    /// A key for a well-formed trust domain and tid, the only kind that may name
    /// a directory.
    pub fn new(trust_domain: &str, tid: &str) -> Result<Self, StoreError> {
        if !is_tid(tid) {
            return Err(StoreError::Invalid(format!("{tid:?} is not a tid (8 of [0-9a-z])")));
        }
        if !is_trust_domain(trust_domain) {
            return Err(StoreError::Invalid(format!("{trust_domain:?} is not a trust domain")));
        }
        Ok(Self { trust_domain: trust_domain.to_string(), tid: tid.to_string() })
    }
}

impl std::fmt::Display for LogKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.trust_domain, self.tid)
    }
}

/// A tid: exactly 8 characters of `[0-9a-z]` (kernel's Tenant.tid).
pub fn is_tid(s: &str) -> bool {
    s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

/// A trust domain as SPIFFE has it: lower-case letters, digits, `.`, `-` and `_`.
pub fn is_trust_domain(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s != "."
        && s != ".."
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'_')
}

#[derive(Debug)]
pub enum StoreError {
    /// The append is not at the next seq; `last` is the last seq the log holds.
    OutOfOrder { last: u64 },
    /// The tenant was deleted.
    Deleted,
    Invalid(String),
    Corrupt(String),
    Io(io::Error),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::OutOfOrder { last } => write!(f, "out of order: the log's last seq is {last}"),
            StoreError::Deleted => write!(f, "the tenant was deleted"),
            StoreError::Invalid(m) => write!(f, "invalid: {m}"),
            StoreError::Corrupt(m) => write!(f, "corrupt log: {m}"),
            StoreError::Io(e) => write!(f, "i/o: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

const TAIL: &str = "tail";
const SNAPSHOT: &str = "snapshot";
const SHIPPED: &str = "shipped";
const DELETED: &str = "deleted";
const DELETION_UNSHIPPED: &str = "deletion-unshipped";

/// Every tenant's log, under one root.
pub struct Store {
    root: PathBuf,
    logs: Mutex<HashMap<LogKey, Arc<Mutex<TenantLog>>>>,
}

/// One open log. Its mutex orders everything done to it.
struct TenantLog {
    dir: PathBuf,
    deleted: bool,
    snapshot_seq: u64,
    /// (seq, offset) of each tail frame, in order.
    index: Vec<(u64, u64)>,
    tail: Option<File>,
    shipped: u64,
    /// A failed append left bytes it could not remove.
    broken: bool,
}

impl TenantLog {
    fn last_seq(&self) -> u64 {
        self.index.last().map(|(s, _)| *s).unwrap_or(self.snapshot_seq)
    }
}

impl Store {
    /// Opens (creating) the store at `root`.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root, logs: Mutex::new(HashMap::new()) })
    }

    fn log(&self, key: &LogKey) -> Result<Arc<Mutex<TenantLog>>, StoreError> {
        let mut logs = self.logs.lock().unwrap();
        if let Some(l) = logs.get(key) {
            return Ok(l.clone());
        }
        let l = Arc::new(Mutex::new(load(self.root.join(&key.trust_domain).join(&key.tid))?));
        logs.insert(key.clone(), l.clone());
        Ok(l)
    }

    /// Appends the operation at `seq`, which must be one more than the last seq
    /// the log holds, and returns once it is on disk.
    pub fn append(&self, key: &LogKey, seq: u64, changes: Vec<RedoChange>) -> Result<(), StoreError> {
        let log = self.log(key)?;
        let mut log = log.lock().unwrap();
        if log.deleted {
            return Err(StoreError::Deleted);
        }
        let last = log.last_seq();
        if seq != last + 1 {
            return Err(StoreError::OutOfOrder { last });
        }
        if log.broken {
            return Err(StoreError::Io(io::Error::other("a failed append could not be undone; the log reopens on a restart")));
        }
        if log.tail.is_none() {
            create_dirs(&log.dir)?;
            let f = OpenOptions::new().create(true).append(true).open(log.dir.join(TAIL))?;
            sync_dir(&log.dir)?;
            log.tail = Some(f);
        }
        let frame = frame(&ShippedOperation { seq, changes, ..Default::default() });
        let f = log.tail.as_mut().unwrap();
        let offset = f.seek(SeekFrom::End(0))?;
        if let Err(e) = f.write_all(&frame).and_then(|_| f.sync_data()) {
            // Leave no half frame behind a later append, or take no more.
            if f.set_len(offset).is_err() {
                log.broken = true;
            }
            return Err(e.into());
        }
        log.index.push((seq, offset));
        Ok(())
    }

    /// The last seq the log holds: 0 for an empty log.
    #[cfg(test)]
    pub fn last_seq(&self, key: &LogKey) -> Result<u64, StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        if log.deleted {
            return Err(StoreError::Deleted);
        }
        Ok(log.last_seq())
    }

    /// How many operations the tail holds behind the snapshot.
    pub fn tail_len(&self, key: &LogKey) -> Result<usize, StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        Ok(log.index.len())
    }

    pub fn is_deleted(&self, key: &LogKey) -> Result<bool, StoreError> {
        Ok(self.log(key)?.lock().unwrap().deleted)
    }

    /// The tail's operations after seq `after`, at most `max`, in order. An
    /// operation already folded into the snapshot is not there any more: the
    /// first one returned is then later than `after + 1`.
    pub fn read_tail(&self, key: &LogKey, after: u64, max: usize) -> Result<Vec<ShippedOperation>, StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        if log.deleted {
            return Err(StoreError::Deleted);
        }
        let start = log.index.partition_point(|(s, _)| *s <= after);
        let mut out = Vec::new();
        if start == log.index.len() || max == 0 {
            return Ok(out);
        }
        let mut f = File::open(log.dir.join(TAIL))?;
        f.seek(SeekFrom::Start(log.index[start].1))?;
        for _ in start..log.index.len().min(start + max) {
            match read_whole_frame(&mut f, "the tail")? {
                Some(op) => out.push(op),
                None => return Err(StoreError::Corrupt(format!("{}: the tail is shorter than its index", log.dir.display()))),
            }
        }
        Ok(out)
    }

    /// The log as a replay: the snapshot's entries (one PUT each, at the
    /// snapshot's seq), then the tail behind it. Taken under the log's lock, so
    /// a compaction cannot split it; read after it, so appends are not held up.
    pub fn replay(&self, key: &LogKey) -> Result<Replay, StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        if log.deleted {
            return Err(StoreError::Deleted);
        }
        let snapshot = match File::open(log.dir.join(SNAPSHOT)) {
            Ok(mut f) => {
                read_whole_frame(&mut f, "the snapshot")?; // the header
                Some(f)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let tail = match log.index.first() {
            None => None,
            Some((_, offset)) => {
                let mut f = File::open(log.dir.join(TAIL))?;
                f.seek(SeekFrom::Start(*offset))?;
                Some(f)
            }
        };
        Ok(Replay { snapshot, tail, tail_left: log.index.len() })
    }

    /// Folds the tail's operations up to seq `upto` into the snapshot, so a
    /// replay reads one entry per live record instead of every change; only
    /// when there are at least `min_fold` of them, since it rewrites the
    /// snapshot whole. Returns whether it folded.
    pub fn compact(&self, key: &LogKey, upto: u64, min_fold: usize) -> Result<bool, StoreError> {
        let log = self.log(key)?;
        let mut log = log.lock().unwrap();
        if log.deleted {
            return Ok(false);
        }
        // The newest operation is never folded, so a replay always ends at the
        // log's last seq, even when nothing is live (the next append follows it).
        let upto = upto.min(log.last_seq().saturating_sub(1));
        let fold = log.index.partition_point(|(s, _)| *s <= upto);
        if fold == 0 || fold < min_fold {
            return Ok(false);
        }

        // The entries: the snapshot's, then each folded operation's changes.
        let mut entries: BTreeMap<(String, EntryKey), RedoChange> = BTreeMap::new();
        if let Ok(mut f) = File::open(log.dir.join(SNAPSHOT)) {
            read_whole_frame(&mut f, "the snapshot")?;
            while let Some(op) = read_whole_frame(&mut f, "the snapshot")? {
                for c in op.changes {
                    entries.insert(entry_key(&c)?, c);
                }
            }
        }
        // From the first indexed frame: a crashed compaction may have left
        // folded ones before it.
        let mut tail = File::open(log.dir.join(TAIL))?;
        tail.seek(SeekFrom::Start(log.index[0].1))?;
        let mut rest = Vec::new();
        for i in 0..log.index.len() {
            let Some(op) = read_whole_frame(&mut tail, "the tail")? else {
                return Err(StoreError::Corrupt(format!("{}: the tail is shorter than its index", log.dir.display())));
            };
            if i < fold {
                for c in op.changes {
                    let k = entry_key(&c)?;
                    if c.op == RedoOp::Delete as i32 {
                        entries.remove(&k);
                    } else {
                        entries.insert(k, c);
                    }
                }
            } else {
                rest.push(op);
            }
        }
        let seq = log.index[fold - 1].0;

        // The new snapshot first: a crash before the tail is rewritten leaves
        // folded operations in the tail, which a load skips by seq.
        let mut buf = frame(&ShippedOperation { seq, changes: vec![], ..Default::default() });
        for (_, c) in entries {
            buf.extend(frame(&ShippedOperation { seq, changes: vec![c], ..Default::default() }));
        }
        replace(&log.dir, SNAPSHOT, &buf)?;
        log.snapshot_seq = seq;
        // Until the tail is rewritten its folded frames stay in the file, which
        // the index no longer counts (a replay and a load skip them by seq).
        log.index.drain(..fold);

        let mut buf = Vec::new();
        let mut index = Vec::with_capacity(rest.len());
        for op in rest {
            index.push((op.seq, buf.len() as u64));
            buf.extend(frame(&op));
        }
        replace(&log.dir, TAIL, &buf)?;
        log.tail = Some(OpenOptions::new().append(true).open(log.dir.join(TAIL))?);
        log.index = index;
        Ok(true)
    }

    /// Deletes the tenant's log (snapshot and tail) and refuses the tenant from
    /// then on. `unshipped` records that the second site still has to delete
    /// it. Returns false when it was already deleted (deletion is idempotent).
    pub fn delete(&self, key: &LogKey, unshipped: bool) -> Result<bool, StoreError> {
        let log = self.log(key)?;
        let mut log = log.lock().unwrap();
        if log.deleted {
            return Ok(false);
        }
        create_dirs(&log.dir)?;
        if unshipped {
            write_synced(&log.dir.join(DELETION_UNSHIPPED), b"")?;
        }
        // The tombstone before the data: a crash in between leaves a deleted
        // tenant whose files the next load removes.
        write_synced(&log.dir.join(DELETED), b"")?;
        sync_dir(&log.dir)?;
        log.deleted = true;
        log.tail = None;
        log.index.clear();
        log.snapshot_seq = 0;
        log.shipped = 0;
        remove_data(&log.dir)?;
        Ok(true)
    }

    /// The last seq the second site acked.
    pub fn shipped(&self, key: &LogKey) -> Result<u64, StoreError> {
        Ok(self.log(key)?.lock().unwrap().shipped)
    }

    /// Records that the second site has every operation up to `seq`.
    pub fn set_shipped(&self, key: &LogKey, seq: u64) -> Result<(), StoreError> {
        let log = self.log(key)?;
        let mut log = log.lock().unwrap();
        if log.deleted || seq == log.shipped {
            return Ok(());
        }
        replace(&log.dir, SHIPPED, seq.to_string().as_bytes())?;
        log.shipped = seq;
        Ok(())
    }

    /// Whether the tenant was deleted and the second site has not acked it.
    pub fn deletion_unshipped(&self, key: &LogKey) -> Result<bool, StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        Ok(log.deleted && log.dir.join(DELETION_UNSHIPPED).exists())
    }

    pub fn set_deletion_shipped(&self, key: &LogKey) -> Result<(), StoreError> {
        let log = self.log(key)?;
        let log = log.lock().unwrap();
        match fs::remove_file(log.dir.join(DELETION_UNSHIPPED)) {
            Ok(()) => Ok(sync_dir(&log.dir)?),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// The tenants of `trust_domain` that have a log here, deleted ones included.
    pub fn tenants(&self, trust_domain: &str) -> Result<Vec<LogKey>, StoreError> {
        let mut out = Vec::new();
        let dir = self.root.join(trust_domain);
        let rd = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for e in rd {
            let name = e?.file_name();
            if let Some(tid) = name.to_str().filter(|t| is_tid(t)) {
                out.push(LogKey::new(trust_domain, tid)?);
            }
        }
        out.sort();
        Ok(out)
    }
}

/// A replay in progress: the snapshot's entries, then the tail's operations.
pub struct Replay {
    snapshot: Option<File>,
    tail: Option<File>,
    tail_left: usize,
}

impl Iterator for Replay {
    type Item = Result<RedoReplayEntry, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(f) = self.snapshot.as_mut() {
            match read_whole_frame(f, "the snapshot") {
                Ok(Some(op)) => {
                    return Some(Ok(RedoReplayEntry { seq: op.seq, changes: op.changes, from_snapshot: true }));
                }
                Ok(None) => self.snapshot = None,
                Err(e) => return Some(self.fail(e)),
            }
        }
        // The tail as indexed when the replay began, from its first indexed
        // frame: a later append is not in it, and neither are folded operations
        // a crashed compaction left before it.
        while self.tail_left > 0 {
            let op = match read_whole_frame(self.tail.as_mut()?, "the tail") {
                Ok(Some(op)) => op,
                Ok(None) => return Some(self.fail(StoreError::Corrupt("the tail is shorter than its index".into()))),
                Err(e) => return Some(self.fail(e)),
            };
            self.tail_left -= 1;
            return Some(Ok(RedoReplayEntry { seq: op.seq, changes: op.changes, from_snapshot: false }));
        }
        None
    }
}

impl Replay {
    /// Ends the replay at its first error.
    fn fail(&mut self, e: StoreError) -> Result<RedoReplayEntry, StoreError> {
        self.snapshot = None;
        self.tail_left = 0;
        Err(e)
    }
}

/// An entry's identity for compaction: its record type and key, the key's case
/// and value alike (a string "1" and an int 1 are two entries).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EntryKey {
    String(String),
    Int(i64),
    Uint(u64),
}

fn entry_key(c: &RedoChange) -> Result<(String, EntryKey), StoreError> {
    let k = match c.key.as_ref().and_then(|k| k.key.as_ref()) {
        Some(redo_key::Key::StringKey(s)) => EntryKey::String(s.clone()),
        Some(redo_key::Key::IntKey(i)) => EntryKey::Int(*i),
        Some(redo_key::Key::UintKey(u)) => EntryKey::Uint(*u),
        None => return Err(StoreError::Corrupt(format!("a change to {} has no key", c.record_type))),
    };
    Ok((c.record_type.clone(), k))
}

enum Frame {
    Op(ShippedOperation),
    /// A clean end of file.
    End,
    Bad(Damage),
}

enum Damage {
    /// The file ends inside the frame.
    Short,
    /// The header fails its CRC.
    Header,
    /// The body fails its CRC.
    Body,
}

fn frame(op: &ShippedOperation) -> Vec<u8> {
    let body = op.encode_to_vec();
    let len = (body.len() as u32).to_le_bytes();
    let mut out = Vec::with_capacity(12 + body.len());
    out.extend(len);
    out.extend(crc32fast::hash(&len).to_le_bytes());
    out.extend(crc32fast::hash(&body).to_le_bytes());
    out.extend(body);
    out
}

fn read_frame(f: &mut File) -> Result<Frame, StoreError> {
    let mut header = [0u8; 12];
    let n = read_full(f, &mut header)?;
    if n == 0 {
        return Ok(Frame::End);
    }
    if n < 12 {
        return Ok(Frame::Bad(Damage::Short));
    }
    let word = |i: usize| u32::from_le_bytes(header[i..i + 4].try_into().unwrap());
    if crc32fast::hash(&header[..4]) != word(4) {
        return Ok(Frame::Bad(Damage::Header));
    }
    let mut body = vec![0u8; word(0) as usize];
    if read_full(f, &mut body)? < body.len() {
        return Ok(Frame::Bad(Damage::Short));
    }
    if crc32fast::hash(&body) != word(8) {
        return Ok(Frame::Bad(Damage::Body));
    }
    ShippedOperation::decode(body.as_slice())
        .map(Frame::Op)
        .map_err(|e| StoreError::Corrupt(format!("undecodable frame: {e}")))
}

/// Reads a frame that must be whole: any damage is corruption.
fn read_whole_frame(f: &mut File, what: &str) -> Result<Option<ShippedOperation>, StoreError> {
    match read_frame(f)? {
        Frame::Op(op) => Ok(Some(op)),
        Frame::End => Ok(None),
        Frame::Bad(_) => Err(StoreError::Corrupt(format!("{what} has a damaged frame"))),
    }
}

fn read_full(f: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

/// Loads a tenant's log from its directory, cutting a torn last frame off the tail.
fn load(dir: PathBuf) -> Result<TenantLog, StoreError> {
    let mut log = TenantLog { dir, deleted: false, snapshot_seq: 0, index: Vec::new(), tail: None, shipped: 0, broken: false };
    if log.dir.join(DELETED).exists() {
        log.deleted = true;
        remove_data(&log.dir)?;
        return Ok(log);
    }
    match File::open(log.dir.join(SNAPSHOT)) {
        Ok(mut f) => match read_whole_frame(&mut f, "the snapshot")? {
            Some(h) => log.snapshot_seq = h.seq,
            None => return Err(StoreError::Corrupt(format!("{}: snapshot has no header", log.dir.display()))),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    match fs::read_to_string(log.dir.join(SHIPPED)) {
        Ok(s) => {
            log.shipped = s.trim().parse().map_err(|_| StoreError::Corrupt(format!("{}: bad shipped seq", log.dir.display())))?
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let path = log.dir.join(TAIL);
    let mut f = match OpenOptions::new().read(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(log),
        Err(e) => return Err(e.into()),
    };
    let size = f.metadata()?.len();
    let mut offset = 0u64;
    let mut last = log.snapshot_seq;
    loop {
        match read_frame(&mut f)? {
            Frame::Op(op) => {
                let next = f.stream_position()?;
                // Operations a crashed compaction left behind the new snapshot.
                if op.seq > log.snapshot_seq {
                    if op.seq != last + 1 {
                        return Err(StoreError::Corrupt(format!(
                            "{}: seq {} follows {}",
                            log.dir.display(),
                            op.seq,
                            last
                        )));
                    }
                    log.index.push((op.seq, offset));
                    last = op.seq;
                }
                offset = next;
            }
            Frame::End => break,
            Frame::Bad(damage) => {
                // Only an append that never returned (so was never acked) may be
                // cut: a frame the file ends inside, a header the end of the file
                // zero-filled, or a last frame whose body is not all there.
                let torn = match damage {
                    Damage::Short => true,
                    Damage::Header => zeros_from(&mut f, offset)?,
                    Damage::Body => f.stream_position()? == size,
                };
                if !torn {
                    return Err(StoreError::Corrupt(format!("{}: bad frame at {}", log.dir.display(), offset)));
                }
                tracing::warn!(dir = %log.dir.display(), offset, "cutting a torn last frame off the tail");
                f.set_len(offset)?;
                f.sync_all()?;
                break;
            }
        }
    }
    log.tail = Some(f);
    Ok(log)
}

/// Whether everything from `offset` to the end of the file is zero.
fn zeros_from(f: &mut File, offset: u64) -> io::Result<bool> {
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; 8192];
    loop {
        match f.read(&mut buf)? {
            0 => return Ok(true),
            n if buf[..n].iter().any(|b| *b != 0) => return Ok(false),
            _ => {}
        }
    }
}

/// Replaces `dir/name` with `data` whole: a temporary file, fsync, rename, and
/// the directory's fsync.
fn replace(dir: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    create_dirs(dir)?;
    let tmp = dir.join(format!("{name}.tmp"));
    write_synced(&tmp, data)?;
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

fn write_synced(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut f = File::create(path)?;
    f.write_all(data)?;
    f.sync_all()
}

/// Creates a tenant's directory (`<root>/<trust domain>/<tid>`), and makes its
/// entry, and its trust domain's, durable.
fn create_dirs(dir: &Path) -> io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    fs::create_dir_all(dir)?;
    let td = dir.parent().expect("a tenant directory has a trust domain directory");
    sync_dir(td)?;
    sync_dir(td.parent().expect("a trust domain directory has a root"))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn remove_data(dir: &Path) -> io::Result<()> {
    let mut removed = false;
    for name in [TAIL, SNAPSHOT, SHIPPED, "tail.tmp", "snapshot.tmp", "shipped.tmp"] {
        match fs::remove_file(dir.join(name)) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
