//! The redo service: `redolog.Redo` for every tenant's Stratum, and
//! `redo.RedoSite` for the redo of another deployment.
//!
//! - **Order.** Each tenant's log takes an operation only at the next seq.
//! - **Acks.** DURABILITY_LOCAL once the operation is on this site's disk;
//!   DURABILITY_REMOTE once the second site has it too (the shipper, ship.rs).
//!   MEMORY is never appended; REMOTE is refused while no second site is
//!   configured rather than left waiting.
//! - **Isolation.** The tenant a write belongs to is the caller's verified
//!   `/tenants/<tid>` namespace, never a request field, and a log is keyed by
//!   this site's trust domain and that tid.
//! - **Compaction.** A tail of `compact_after` operations is folded into the
//!   tenant's snapshot, never past what the second site acked.
//! - **Deletion.** Kernel's tombstone deletes the tenant's log here at once and
//!   is shipped to the second site, and the tenant is refused from then on.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use redo_model::s2sm::internal::grpc::servers::services::redo::site::RedoSiteHandler;
use redo_model::s2sm::internal::grpc::servers::sys::redolog::redo::RedoHandler;
use redo_model::types::services::kernel::TenantIdRow;
use redo_model::types::services::redo::redo::{shipment, Shipment, ShipmentAck};
use redo_model::types::sys::redolog::{RedoAck, RedoAppend, RedoChange, RedoOp, RedoReplayEntry, RedoReplayRequest};
use redo_model::types::sys::spec::Durability;
use servicemanager::auth::Caller;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

use crate::ship::ShipQueue;
use crate::site::SecondSite;
use crate::store::{is_tid, is_trust_domain, LogKey, Store, StoreError};

/// The reserved restore tenant, which the redo service runs in, in every deployment.
pub(crate) const RESTORE_TID: &str = "00000002";

type BoxStream<T> = std::pin::Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

pub struct Options {
    /// This deployment's trust domain: every log of its own tenants is keyed by it.
    pub trust_domain: String,
    pub data_dir: PathBuf,
    pub compact_after: u64,
    /// The redo at the second site; None until the deployed link exists
    /// (tacplus-dev/tacplus#204).
    pub second_site: Option<Arc<dyn SecondSite>>,
    /// The shipper's wait before reaching the second site again, doubling up to retry_max.
    pub retry_min: Duration,
    pub retry_max: Duration,
}

/// What the second site has of one tenant's log, as REMOTE acks wait on it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Shipped {
    pub seq: u64,
    pub deleted: bool,
    /// Why the second site can never have this tenant's log, when it cannot.
    pub failed: Option<String>,
}

/// One site's redo.
pub struct Core {
    pub(crate) trust_domain: String,
    data_dir: PathBuf,
    pub(crate) store: Arc<Store>,
    compact_after: AtomicU64,
    retry_max: Duration,
    pub(crate) ship_queue: Option<Arc<ShipQueue>>,
    shipped: Mutex<HashMap<LogKey, watch::Sender<Shipped>>>,
    compacting: Mutex<HashSet<LogKey>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Core {
    /// Opens the store and, with a second site, starts shipping to it.
    pub fn start(o: Options) -> Result<Arc<Core>, StoreError> {
        if !is_trust_domain(&o.trust_domain) {
            return Err(StoreError::Invalid(format!("{:?} is not a trust domain", o.trust_domain)));
        }
        let store = Arc::new(Store::open(&o.data_dir)?);
        let core = Arc::new(Core {
            trust_domain: o.trust_domain,
            data_dir: o.data_dir,
            store,
            compact_after: AtomicU64::new(o.compact_after.max(1)),
            retry_max: o.retry_max,
            ship_queue: o.second_site.as_ref().map(|_| Arc::new(ShipQueue::default())),
            shipped: Mutex::new(HashMap::new()),
            compacting: Mutex::new(HashSet::new()),
            tasks: Mutex::new(Vec::new()),
        });
        if let Some(site) = o.second_site {
            let task = tokio::spawn(crate::ship::run(core.clone(), site, o.retry_min, o.retry_max));
            core.tasks.lock().unwrap().push(task);
        }
        Ok(core)
    }

    pub fn set_compact_after(&self, n: u64) {
        self.compact_after.store(n.max(1), Ordering::Relaxed);
    }

    /// Stops shipping. The logs are on disk; nothing else is left to drain.
    pub async fn shutdown(&self) {
        let tasks: Vec<_> = self.tasks.lock().unwrap().drain(..).collect();
        for t in tasks {
            t.abort();
            let _ = t.await;
        }
    }

    /// Applies kernel's tenant ids (ConfigState.tenant_ids): a tombstoned
    /// tenant's log is deleted here, and its deletion queued for the second
    /// site. Watch events repeat the whole set; a tenant deleted already is left
    /// as it is.
    pub fn apply_tenant_ids(self: &Arc<Self>, rows: Vec<TenantIdRow>) {
        for row in rows.iter().filter(|r| r.tombstoned_at_ms != 0) {
            if !is_tid(&row.tid) {
                tracing::warn!(tid = %row.tid, "a tombstoned tenant id that is not a tid; ignored");
                continue;
            }
            let key = match LogKey::new(&self.trust_domain, &row.tid) {
                Ok(k) => k,
                Err(e) => {
                    tracing::error!(error = %e, "tenant deletion");
                    continue;
                }
            };
            match self.store.delete(&key, true) {
                Ok(true) => {
                    tracing::info!(tenant = %key, "tenant deleted: its log is gone here, and goes at the second site");
                    self.shipped_sender(&key).send_modify(|s| s.deleted = true);
                    self.mark_for_shipping(&key);
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(tenant = %key, error = %e, retry_in = ?self.retry_max, "could not delete the tenant's log");
                    // Watch events come only when the tenant ids change.
                    if let Ok(rt) = tokio::runtime::Handle::try_current() {
                        let (core, row) = (self.clone(), row.clone());
                        rt.spawn(async move {
                            tokio::time::sleep(core.retry_max).await;
                            core.apply_tenant_ids(vec![row]);
                        });
                    }
                }
            }
        }
    }

    /// The tenant the caller writes and reads as: its verified `/tenants/<tid>`
    /// namespace, nothing below or beside it.
    fn caller_key(&self, caller: Option<&Caller>) -> Result<LogKey, Status> {
        let caller = caller.ok_or_else(|| Status::permission_denied("the redo log needs the caller's tenant identity"))?;
        let ns = caller
            .token()
            .namespace_path()
            .map_err(|e| Status::permission_denied(format!("the caller's namespace: {e}")))?;
        let tid = ns
            .strip_prefix("/tenants/")
            .filter(|t| is_tid(t))
            .ok_or_else(|| Status::permission_denied(format!("the caller's namespace {ns:?} is not a tenant's (/tenants/<tid>)")))?;
        LogKey::new(&self.trust_domain, tid).map_err(|e| Status::internal(e.to_string()))
    }

    fn shipped_sender(&self, key: &LogKey) -> watch::Sender<Shipped> {
        let mut m = self.shipped.lock().unwrap();
        if let Some(s) = m.get(key) {
            return s.clone();
        }
        let initial = Shipped {
            seq: self.store.shipped(key).unwrap_or(0),
            deleted: self.store.is_deleted(key).unwrap_or(false),
            failed: None,
        };
        let s = watch::Sender::new(initial);
        m.insert(key.clone(), s.clone());
        s
    }

    /// Records that the second site holds `key`'s log up to `seq`, for the
    /// REMOTE acks waiting on it.
    pub(crate) fn set_remote(&self, key: &LogKey, seq: u64) {
        self.shipped_sender(key).send_modify(|s| s.seq = seq);
    }

    /// Fails the REMOTE acks waiting on `key`, and those to come: the second
    /// site can never have its log.
    pub(crate) fn set_remote_failed(&self, key: &LogKey, why: &str) {
        self.shipped_sender(key).send_modify(|s| s.failed = Some(why.to_string()));
    }

    pub(crate) fn mark_for_shipping(&self, key: &LogKey) {
        if let Some(q) = &self.ship_queue {
            q.mark(key.clone());
        }
    }

    /// Folds the tenant's tail into its snapshot once it holds `compact_after`
    /// operations that may be folded: with a second site, only those it acked.
    pub(crate) fn maybe_compact(self: &Arc<Self>, key: &LogKey) {
        let min_fold = self.compact_after.load(Ordering::Relaxed) as usize;
        match self.store.tail_len(key) {
            Ok(n) if n >= min_fold => {}
            _ => return,
        }
        if !self.compacting.lock().unwrap().insert(key.clone()) {
            return;
        }
        let core = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let upto = if core.ship_queue.is_some() && key.trust_domain == core.trust_domain {
                core.store.shipped(&key).unwrap_or(0)
            } else {
                u64::MAX
            };
            match core.store.compact(&key, upto, min_fold) {
                Ok(true) => tracing::debug!(tenant = %key, "compacted"),
                Ok(false) => {}
                Err(e) => tracing::error!(tenant = %key, error = %e, "compaction failed"),
            }
            core.compacting.lock().unwrap().remove(&key);
        });
    }

    /// Appends one operation of the caller's tenant; returns whether it is owed
    /// a REMOTE ack.
    async fn append_one(self: &Arc<Self>, key: &LogKey, m: RedoAppend) -> Result<bool, Status> {
        let remote = match Durability::try_from(m.durability) {
            Ok(Durability::Unspecified | Durability::Local) => false,
            Ok(Durability::Remote) => true,
            Ok(Durability::Memory) => {
                return Err(Status::invalid_argument("a DURABILITY_MEMORY write is never appended"));
            }
            Err(_) => return Err(Status::invalid_argument(format!("unknown durability {}", m.durability))),
        };
        if remote && self.ship_queue.is_none() {
            return Err(Status::failed_precondition(
                "no second site is configured, so a DURABILITY_REMOTE write cannot be acked",
            ));
        }
        check_changes(&m.changes)?;
        let (store, k, seq) = (self.store.clone(), key.clone(), m.seq);
        tokio::task::spawn_blocking(move || store.append(&k, seq, m.changes))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| store_status(e, seq))?;
        self.mark_for_shipping(key);
        self.maybe_compact(key);
        Ok(remote)
    }

    /// Takes one shipment from the source site.
    async fn receive(self: &Arc<Self>, m: Shipment) -> Result<ShipmentAck, Status> {
        if m.source_trust_domain == self.trust_domain {
            return Err(Status::permission_denied("a site never receives its own trust domain's logs"));
        }
        let key = LogKey::new(&m.source_trust_domain, &m.tid).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let body = m.body.ok_or_else(|| Status::invalid_argument("a shipment carries an operation or a deletion"))?;
        if let shipment::Body::Operation(op) = &body {
            check_changes(&op.changes)?;
        }
        let ack = |seq, deleted| ShipmentAck {
            source_trust_domain: key.trust_domain.clone(),
            tid: key.tid.clone(),
            seq,
            deleted,
            ..Default::default()
        };
        let (store, k) = (self.store.clone(), key.clone());
        let held = tokio::task::spawn_blocking(move || match body {
            shipment::Body::Deletion(_) => store.delete(&k, false).map(|_| None),
            shipment::Body::Operation(op) => match store.append(&k, op.seq, op.changes) {
                Ok(()) => Ok(Some(op.seq)),
                // A resend, or past a gap: the source goes on from what is held.
                Err(StoreError::OutOfOrder { last }) => Ok(Some(last)),
                Err(StoreError::Deleted) => Ok(None),
                Err(e) => Err(e),
            },
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| store_status(e, 0))?;
        Ok(match held {
            Some(seq) => {
                self.maybe_compact(&key);
                ack(seq, false)
            }
            None => ack(0, true),
        })
    }
}

/// Every change names its entry and does something to it.
fn check_changes(changes: &[RedoChange]) -> Result<(), Status> {
    if changes.is_empty() {
        return Err(Status::invalid_argument("an operation has at least one change"));
    }
    for c in changes {
        if c.key.as_ref().and_then(|k| k.key.as_ref()).is_none() {
            return Err(Status::invalid_argument(format!("a change to {} has no key", c.record_type)));
        }
        if c.op != RedoOp::Put as i32 && c.op != RedoOp::Delete as i32 {
            return Err(Status::invalid_argument(format!("a change to {} is neither a PUT nor a DELETE", c.record_type)));
        }
    }
    Ok(())
}

fn store_status(e: StoreError, seq: u64) -> Status {
    match e {
        StoreError::OutOfOrder { last } => {
            Status::failed_precondition(format!("seq {seq} is not the next: the log's last seq is {last}"))
        }
        StoreError::Deleted => Status::failed_precondition("the tenant was deleted"),
        StoreError::Invalid(m) => Status::invalid_argument(m),
        StoreError::Corrupt(m) => Status::internal(format!("the tenant's log is corrupt: {m}")),
        // The disk is this site's durability: no ack without it.
        StoreError::Io(e) => Status::unavailable(format!("the log could not be written: {e}")),
    }
}

fn ack(seq: u64, level: Durability) -> RedoAck {
    RedoAck { seq, level: level as i32, ..Default::default() }
}

/// `redolog.Redo` for every tenant's Stratum.
pub struct RedoServer(pub Arc<Core>);

#[tonic::async_trait]
impl RedoHandler for RedoServer {
    async fn append(&self, caller: Option<&Caller>, req: tonic::Request<BoxStream<RedoAppend>>) -> Result<tonic::Response<BoxStream<RedoAck>>, Status> {
        let core = self.0.clone();
        let key = core.caller_key(caller)?;
        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::channel(256);

        // REMOTE acks, in seq order, as the second site acks the log's prefix.
        let (remote_tx, mut remote_rx) = mpsc::unbounded_channel::<u64>();
        let mut shipped = core.shipped_sender(&key).subscribe();
        let remote_acks = tx.clone();
        let waiter = tokio::spawn(async move {
            while let Some(seq) = remote_rx.recv().await {
                let s = match shipped.wait_for(|s| s.deleted || s.failed.is_some() || s.seq >= seq).await {
                    Ok(s) => s.clone(),
                    Err(_) => return,
                };
                let item = if s.deleted {
                    Err(Status::failed_precondition("the tenant was deleted"))
                } else if s.seq >= seq {
                    Ok(ack(seq, Durability::Remote))
                } else {
                    Err(Status::failed_precondition(s.failed.unwrap_or_default()))
                };
                let stop = item.is_err();
                if remote_acks.send(item).await.is_err() || stop {
                    return;
                }
            }
        });

        tokio::spawn(async move {
            while let Some(m) = inbound.next().await {
                let seq = m.as_ref().map(|m| m.seq).unwrap_or(0);
                let r = match m {
                    Ok(m) => core.append_one(&key, m).await,
                    Err(s) => Err(s),
                };
                match r {
                    Ok(remote) => {
                        if tx.send(Ok(ack(seq, Durability::Local))).await.is_err() {
                            break;
                        }
                        if remote {
                            let _ = remote_tx.send(seq);
                        }
                    }
                    Err(s) => {
                        waiter.abort();
                        let _ = tx.send(Err(s)).await;
                        return;
                    }
                }
            }
            // The store closed its side: the REMOTE acks it is owed still come.
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn replay(&self, caller: Option<&Caller>, _req: RedoReplayRequest) -> Result<tonic::Response<BoxStream<RedoReplayEntry>>, Status> {
        let key = self.0.caller_key(caller)?;
        let store = self.0.store.clone();
        let entries = tokio::task::spawn_blocking(move || store.replay(&key))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| store_status(e, 0))?;
        let (tx, rx) = mpsc::channel(256);
        tokio::task::spawn_blocking(move || {
            for e in entries {
                let item = e.map_err(|e| store_status(e, 0));
                let stop = item.is_err();
                if tx.blocking_send(item).is_err() || stop {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// `redo.RedoSite` for the redo of another deployment.
pub struct SiteServer(pub Arc<Core>);

#[tonic::async_trait]
impl RedoSiteHandler for SiteServer {
    async fn append(&self, caller: Option<&Caller>, req: tonic::Request<BoxStream<Shipment>>) -> Result<tonic::Response<BoxStream<ShipmentAck>>, Status> {
        // The generated authorizer admits the redo service only; this is the
        // redo of the restore tenant, the only one there is.
        let ns = caller
            .ok_or_else(|| Status::permission_denied("shipping needs the caller's identity"))?
            .token()
            .namespace_path()
            .map_err(|e| Status::permission_denied(format!("the caller's namespace: {e}")))?;
        if ns != format!("/tenants/{RESTORE_TID}") {
            return Err(Status::permission_denied(format!("shipments come from the restore tenant's redo, not {ns:?}")));
        }
        let core = self.0.clone();
        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(m) = inbound.next().await {
                let r = match m {
                    Ok(m) => core.receive(m).await,
                    Err(s) => Err(s),
                };
                let stop = r.is_err();
                if tx.send(r).await.is_err() || stop {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// The service as the ServiceManager drives it: the generated server asks for
/// the handlers before the first config opens the store, and kernel's tenant
/// ids may arrive before it too.
pub struct RedoService {
    trust_domain: String,
    second_site: Option<Arc<dyn SecondSite>>,
    core: OnceLock<Arc<Core>>,
    /// Tenant ids that arrived before the store opened; guards `core`'s setting.
    early_tenant_ids: Mutex<Option<Vec<TenantIdRow>>>,
}

impl RedoService {
    pub fn new(trust_domain: String, second_site: Option<Arc<dyn SecondSite>>) -> Arc<Self> {
        Arc::new(Self { trust_domain, second_site, core: OnceLock::new(), early_tenant_ids: Mutex::new(None) })
    }

    pub fn core(&self) -> Option<Arc<Core>> {
        self.core.get().cloned()
    }

    /// Applies a config: the first opens the store at `data_dir`; later ones
    /// change only `compact_after` (the volume does not move under a running
    /// instance).
    pub fn configure(&self, data_dir: PathBuf, compact_after: u64) -> Result<(), StoreError> {
        let mut early = self.early_tenant_ids.lock().unwrap();
        if let Some(core) = self.core.get() {
            if core.data_dir != data_dir {
                tracing::warn!(data_dir = %data_dir.display(), "data_dir changes only on a restart");
            }
            core.set_compact_after(compact_after);
            return Ok(());
        }
        let core = Core::start(Options {
            trust_domain: self.trust_domain.clone(),
            data_dir,
            compact_after,
            second_site: self.second_site.clone(),
            retry_min: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
        })?;
        if let Some(rows) = early.take() {
            core.apply_tenant_ids(rows);
        }
        let _ = self.core.set(core);
        Ok(())
    }

    pub fn on_tenant_ids(&self, rows: Vec<TenantIdRow>) {
        let mut early = self.early_tenant_ids.lock().unwrap();
        match self.core.get() {
            Some(core) => core.apply_tenant_ids(rows),
            None => *early = Some(rows),
        }
    }

    fn serving(&self) -> Result<Arc<Core>, Status> {
        self.core().ok_or_else(|| Status::unavailable("the redo log is not open yet"))
    }

    pub async fn shutdown(&self) {
        if let Some(core) = self.core.get() {
            core.shutdown().await;
        }
    }
}

/// `redolog.Redo` before and after the store opens.
pub struct LazyRedo(pub Arc<RedoService>);

#[tonic::async_trait]
impl RedoHandler for LazyRedo {
    async fn append(&self, caller: Option<&Caller>, req: tonic::Request<BoxStream<RedoAppend>>) -> Result<tonic::Response<BoxStream<RedoAck>>, Status> {
        RedoServer(self.0.serving()?).append(caller, req).await
    }

    async fn replay(&self, caller: Option<&Caller>, req: RedoReplayRequest) -> Result<tonic::Response<BoxStream<RedoReplayEntry>>, Status> {
        RedoServer(self.0.serving()?).replay(caller, req).await
    }
}

/// `redo.RedoSite` before and after the store opens.
pub struct LazySite(pub Arc<RedoService>);

#[tonic::async_trait]
impl RedoSiteHandler for LazySite {
    async fn append(&self, caller: Option<&Caller>, req: tonic::Request<BoxStream<Shipment>>) -> Result<tonic::Response<BoxStream<ShipmentAck>>, Status> {
        SiteServer(self.0.serving()?).append(caller, req).await
    }
}
