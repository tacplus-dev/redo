//! Two sites in one process: callers as the generated server hands them to a
//! handler, and a second site reached through the `SecondSite` interface that
//! can be taken down and cut, to test shipping and catching up.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use redo_model::s2sm::internal::grpc::servers::services::redo::site::RedoSiteHandler;
use redo_model::types::services::redo::redo::Shipment;
use redo_model::types::sys::redolog::{redo_key, RedoAck, RedoChange, RedoKey, RedoOp};
use servicemanager::auth::Caller;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};

use crate::service::{Core, Options, SiteServer};
use crate::site::{SecondSite, ShipmentAcks, Shipments};

/// How long a test waits for what must come: an ack, a replay, a condition.
/// Every append, deletion and compaction is fsynced first, and an fsync can
/// wait seconds on a disk's write-back backlog (a build just before, say): a
/// slow disk must not fail a test. A passing test never waits this long; only
/// a failing one takes this long to report.
pub const DEADLINE: Duration = Duration::from_secs(60);

/// A PUT of `value` at a string key.
pub fn put(record_type: &str, key: &str, value: &[u8]) -> RedoChange {
    RedoChange {
        record_type: record_type.into(),
        key: Some(RedoKey { key: Some(redo_key::Key::StringKey(key.into())) }),
        op: RedoOp::Put as i32,
        value: value.to_vec(),
        revision: 1,
    }
}

/// A DELETE at a string key.
pub fn del(record_type: &str, key: &str) -> RedoChange {
    RedoChange {
        record_type: record_type.into(),
        key: Some(RedoKey { key: Some(redo_key::Key::StringKey(key.into())) }),
        op: RedoOp::Delete as i32,
        value: vec![],
        revision: 2,
    }
}

/// A service caller in `namespace` with no tid claim (a `/system` service's),
/// as the generated server passes it to a handler once the token is verified.
pub fn service_caller(namespace: &str, service: &str) -> Caller {
    tenant_caller(namespace, None, service)
}

/// A service of tenant `tid`, as kernel mints its token: the namespace names
/// the tenant (never by its tid), and the `tid` claim carries its tid.
pub fn tenant_service(tid: &str, service: &str) -> Caller {
    tenant_caller(&format!("/tenants/tenant{tid}"), Some(tid), service)
}

/// A service caller in `namespace` with `tid` as its tid claim, or none, as
/// the generated server passes it to a handler once the token is verified.
pub fn tenant_caller(namespace: &str, tid: Option<&str>, service: &str) -> Caller {
    let mut t = token::token::Token::new();
    t.set_subject(format!("spiffe://td.test{namespace}/services/{service}")).unwrap();
    t.set_namespace(namespace).unwrap();
    t.set_sub_type("service").unwrap();
    if let Some(tid) = tid {
        t.set_tid(tid).unwrap();
    }
    Caller::new(t, "request".into())
}

/// The Stratum of tenant `tid`.
pub fn stratum(tid: &str) -> Caller {
    tenant_service(tid, "stratum")
}

/// The redo of another deployment, a `/system` service: RedoSite's only caller.
pub fn system_redo() -> Caller {
    service_caller("/system", "redo")
}

/// A site's core on `dir`, shipping to `second_site` when there is one.
pub fn start_core(trust_domain: &str, dir: &Path, compact_after: u64, second_site: Option<Arc<dyn SecondSite>>) -> Arc<Core> {
    Core::start(Options {
        trust_domain: trust_domain.into(),
        data_dir: dir.into(),
        compact_after,
        second_site,
        retry_min: Duration::from_millis(10),
        retry_max: Duration::from_millis(50),
    })
    .unwrap()
}

/// The second site, in process: the receiving site's own RedoSite handler,
/// called as the shipping site's `/system` redo. While down it cannot be reached; a cut
/// ends the stream open at the time. It records every shipment it is sent.
pub struct InProcessSite {
    target: Arc<SiteServer>,
    up: AtomicBool,
    cut: watch::Sender<u64>,
    sent: Arc<Mutex<Vec<Shipment>>>,
}

impl InProcessSite {
    pub fn new(target: Arc<Core>) -> Arc<Self> {
        Arc::new(Self {
            target: Arc::new(SiteServer(target)),
            up: AtomicBool::new(true),
            cut: watch::channel(0).0,
            sent: Arc::default(),
        })
    }

    /// The shipments sent to the second site so far, in order.
    pub fn sent(&self) -> Vec<Shipment> {
        self.sent.lock().unwrap().clone()
    }

    /// Takes the site down (cutting the open stream) or brings it back.
    pub fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::SeqCst);
        if !up {
            self.cut();
        }
    }

    /// Ends the stream open now, as a connection lost would.
    pub fn cut(&self) {
        self.cut.send_modify(|n| *n += 1);
    }
}

#[async_trait::async_trait]
impl SecondSite for InProcessSite {
    async fn open(&self, shipments: Shipments) -> Result<ShipmentAcks, tonic::Status> {
        if !self.up.load(Ordering::SeqCst) {
            return Err(tonic::Status::unavailable("the second site is down"));
        }
        let mut cut = self.cut.subscribe();
        cut.mark_unchanged();
        let caller = system_redo();
        let sent = self.sent.clone();
        let inbound = Box::pin(shipments.map(move |s| {
            sent.lock().unwrap().push(s.clone());
            Ok::<Shipment, tonic::Status>(s)
        }));
        let acks = self.target.append(Some(&caller), tonic::Request::new(inbound)).await?.into_inner();
        // Until the cut, the acks; then the end of the stream.
        let cut = WatchStream::from_changes(cut).map(|_| None);
        Ok(Box::pin(acks.map(Some).merge(cut).take_while(|a| a.is_some()).map(|a| a.unwrap())))
    }
}

/// The next ack, failing the test after the `DEADLINE`.
pub async fn next_ack<S>(acks: &mut S) -> Result<RedoAck, tonic::Status>
where
    S: Stream<Item = Result<RedoAck, tonic::Status>> + Unpin,
{
    tokio::time::timeout(DEADLINE, acks.next())
        .await
        .expect("no ack within the deadline")
        .expect("the ack stream ended")
}

/// Asserts that no ack arrives for a while.
pub async fn no_ack_for<S>(acks: &mut S, wait: Duration)
where
    S: Stream<Item = Result<RedoAck, tonic::Status>> + Unpin,
{
    if let Ok(a) = tokio::time::timeout(wait, acks.next()).await {
        panic!("unexpected ack: {a:?}");
    }
}

/// Waits until `cond` holds, failing the test after the `DEADLINE`.
pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while !cond() {
        if tokio::time::Instant::now() >= deadline {
            panic!("not within the deadline: {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
