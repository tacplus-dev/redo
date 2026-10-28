//! The service against its callers: ordering, the ack levels, replay,
//! compaction, tenant isolation, and the second site (shipping, catching up
//! after an outage, deletion at both sites), with a second in-process site.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use redo_model::s2sm::internal::grpc::servers::services::redo::site::RedoSiteHandler;
use redo_model::s2sm::internal::grpc::servers::sys::redolog::redo::RedoHandler;
use redo_model::types::services::kernel::TenantIdRow;
use redo_model::types::services::redo::redo::{shipment, Shipment, ShipmentAck, ShippedDeletion, ShippedOperation};
use redo_model::types::sys::redolog::{redo_key, RedoAck, RedoAppend, RedoChange, RedoOp, RedoReplayEntry, RedoReplayRequest};
use redo_model::types::sys::spec::Durability;
use servicemanager::auth::Caller;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Code;

use crate::service::{Core, RedoServer, SiteServer};
use crate::store::LogKey;
use crate::test_support::*;

const A: &str = "a1234567";
const B: &str = "b1234567";
const ONE: &str = "one.test";
const TWO: &str = "two.test";

type Acks = std::pin::Pin<Box<dyn Stream<Item = Result<RedoAck, tonic::Status>> + Send>>;

/// An open Append stream: what the store sends, and the acks.
struct Appender {
    tx: mpsc::Sender<Result<RedoAppend, tonic::Status>>,
    acks: Acks,
}

impl Appender {
    async fn open(redo: &RedoServer, caller: &Caller) -> Result<Self, tonic::Status> {
        let (tx, rx) = mpsc::channel(64);
        let inbound = Box::pin(ReceiverStream::new(rx));
        let acks = redo.append(Some(caller), tonic::Request::new(inbound)).await?.into_inner();
        Ok(Self { tx, acks })
    }

    async fn send(&self, seq: u64, changes: Vec<RedoChange>, durability: Durability) {
        let a = RedoAppend { tenant: RESTORE.into(), seq, changes, durability: durability as i32, ..Default::default() };
        self.tx.send(Ok(a)).await.unwrap();
    }

    async fn ack(&mut self) -> RedoAck {
        next_ack(&mut self.acks).await.expect("an ack, not an error")
    }

    async fn error(&mut self) -> tonic::Status {
        match next_ack(&mut self.acks).await {
            Err(s) => s,
            Ok(a) => panic!("expected an error, got {a:?}"),
        }
    }
}

fn ack(seq: u64, level: Durability) -> RedoAck {
    RedoAck { seq, level: level as i32, ..Default::default() }
}

async fn replay(redo: &RedoServer, caller: &Caller) -> Result<Vec<RedoReplayEntry>, tonic::Status> {
    let req = RedoReplayRequest { tenant: RESTORE.into(), ..Default::default() };
    let mut s = redo.replay(Some(caller), req).await?.into_inner();
    let mut out = Vec::new();
    while let Some(e) = tokio::time::timeout(Duration::from_secs(5), s.next()).await.expect("replay stalled") {
        out.push(e?);
    }
    Ok(out)
}

/// The entries a replay restores: what a Stratum starting holds.
fn restore(entries: &[RedoReplayEntry]) -> BTreeMap<(String, String), Vec<u8>> {
    let mut state = BTreeMap::new();
    for e in entries {
        for c in &e.changes {
            let Some(redo_key::Key::StringKey(k)) = c.key.as_ref().and_then(|k| k.key.clone()) else { panic!() };
            if c.op == RedoOp::Delete as i32 {
                state.remove(&(c.record_type.clone(), k));
            } else {
                state.insert((c.record_type.clone(), k), c.value.clone());
            }
        }
    }
    state
}

fn seqs(entries: &[RedoReplayEntry]) -> Vec<u64> {
    entries.iter().map(|e| e.seq).collect()
}

fn tombstone(tid: &str) -> TenantIdRow {
    TenantIdRow { tid: tid.into(), tenant_namespace: format!("/tenants/{tid}"), tombstoned_at_ms: 1, ..Default::default() }
}

/// Site one shipping to site two, both in this process.
struct TwoSites {
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    one: Arc<Core>,
    two: Arc<Core>,
    link: Arc<InProcessSite>,
}

impl TwoSites {
    fn start(compact_after: u64) -> Self {
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let two = start_core(TWO, d2.path(), compact_after, None);
        let link = InProcessSite::new(two.clone());
        let one = start_core(ONE, d1.path(), compact_after, Some(link.clone()));
        Self { _dirs: (d1, d2), one, two, link }
    }

    fn redo(&self) -> RedoServer {
        RedoServer(self.one.clone())
    }

    /// Site two's copy of site one's log for `tid`.
    fn copy(&self, tid: &str) -> Vec<RedoReplayEntry> {
        let k = LogKey::new(ONE, tid).unwrap();
        self.two.store.replay(&k).unwrap().collect::<Result<_, _>>().unwrap()
    }

    fn copy_last_seq(&self, tid: &str) -> u64 {
        self.two.store.last_seq(&LogKey::new(ONE, tid).unwrap()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Order and the LOCAL ack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_write_is_acked_local_once_on_disk_in_seq_order() {
    let dir = tempfile::tempdir().unwrap();
    let core = start_core(ONE, dir.path(), 1000, None);
    let redo = RedoServer(core.clone());
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    for seq in 1..=3 {
        s.send(seq, vec![put("t.R", "x", &[seq as u8])], Durability::Local).await;
    }
    for seq in 1..=3 {
        assert_eq!(s.ack().await, ack(seq, Durability::Local));
    }
    // Unset durability is LOCAL.
    s.send(4, vec![put("t.R", "y", b"4")], Durability::Unspecified).await;
    assert_eq!(s.ack().await, ack(4, Durability::Local));
    // On disk: a restarted site replays it all.
    drop(s);
    core.shutdown().await;
    let core = start_core(ONE, dir.path(), 1000, None);
    let got = replay(&RedoServer(core), &stratum(A)).await.unwrap();
    assert_eq!(seqs(&got), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn an_append_out_of_order_is_refused_and_the_store_continues_after_a_replay() {
    let dir = tempfile::tempdir().unwrap();
    let redo = RedoServer(start_core(ONE, dir.path(), 1000, None));
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    s.send(2, vec![put("t.R", "x", b"1")], Durability::Local).await;
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);

    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Local).await;
    assert_eq!(s.ack().await, ack(1, Durability::Local));
    s.send(1, vec![put("t.R", "x", b"again")], Durability::Local).await;
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);

    // A store that lost its place replays, and continues at the next seq.
    let got = replay(&redo, &stratum(A)).await.unwrap();
    let next = got.last().unwrap().seq + 1;
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    s.send(next, vec![put("t.R", "y", b"2")], Durability::Local).await;
    assert_eq!(s.ack().await, ack(2, Durability::Local));
    assert_eq!(restore(&replay(&redo, &stratum(A)).await.unwrap()).len(), 2);
}

#[tokio::test]
async fn memory_durability_and_malformed_changes_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let redo = RedoServer(start_core(ONE, dir.path(), 1000, None));
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Memory).await;
    assert_eq!(s.error().await.code(), Code::InvalidArgument);

    let mut no_key = put("t.R", "x", b"1");
    no_key.key = None;
    let mut no_op = put("t.R", "x", b"1");
    no_op.op = RedoOp::Unspecified as i32;
    for bad in [vec![], vec![no_key], vec![no_op]] {
        let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
        s.send(1, bad, Durability::Local).await;
        assert_eq!(s.error().await.code(), Code::InvalidArgument);
    }
    assert!(replay(&redo, &stratum(A)).await.unwrap().is_empty());
}

#[tokio::test]
async fn remote_is_refused_while_no_second_site_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    let redo = RedoServer(start_core(ONE, dir.path(), 1000, None));
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);
    assert!(replay(&redo, &stratum(A)).await.unwrap().is_empty(), "a refused append is not logged");
}

// ---------------------------------------------------------------------------
// Tenant isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tenant_writes_and_reads_only_its_own_log() {
    let dir = tempfile::tempdir().unwrap();
    let redo = RedoServer(start_core(ONE, dir.path(), 1000, None));
    let mut a = Appender::open(&redo, &stratum(A)).await.unwrap();
    a.send(1, vec![put("t.R", "x", b"a")], Durability::Local).await;
    a.ack().await;

    // B's log is its own: empty, and starting at seq 1.
    assert!(replay(&redo, &stratum(B)).await.unwrap().is_empty());
    let mut b = Appender::open(&redo, &stratum(B)).await.unwrap();
    b.send(1, vec![put("t.R", "x", b"b")], Durability::Local).await;
    b.ack().await;
    assert_eq!(restore(&replay(&redo, &stratum(A)).await.unwrap()).values().next().unwrap(), b"a");
    assert_eq!(restore(&replay(&redo, &stratum(B)).await.unwrap()).values().next().unwrap(), b"b");
}

#[tokio::test]
async fn a_caller_without_a_tenant_identity_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let redo = RedoServer(start_core(ONE, dir.path(), 1000, None));
    let callers = [
        service_caller("/system", "stratum"),
        service_caller("/tenants/acme", "stratum"),
        service_caller(&format!("/tenants/{A}/projects/p"), "stratum"),
        service_caller("/tenants", "stratum"),
    ];
    for c in &callers {
        assert_eq!(Appender::open(&redo, c).await.err().unwrap().code(), Code::PermissionDenied);
        assert_eq!(replay(&redo, c).await.unwrap_err().code(), Code::PermissionDenied);
    }
    let req = RedoReplayRequest { tenant: RESTORE.into(), ..Default::default() };
    assert_eq!(redo.replay(None, req).await.err().unwrap().code(), Code::PermissionDenied);
}

// ---------------------------------------------------------------------------
// Compaction and replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_keeps_replay_short_and_exact() {
    let dir = tempfile::tempdir().unwrap();
    let core = start_core(ONE, dir.path(), 4, None);
    let redo = RedoServer(core.clone());
    let mut s = Appender::open(&redo, &stratum(A)).await.unwrap();
    let mut expected = BTreeMap::new();
    for seq in 1..=10u64 {
        let k = format!("k{}", seq % 3);
        let c = if seq % 4 == 0 { del("t.R", &k) } else { put("t.R", &k, &[seq as u8]) };
        if seq % 4 == 0 {
            expected.remove(&("t.R".to_string(), k));
        } else {
            expected.insert(("t.R".to_string(), k), vec![seq as u8]);
        }
        s.send(seq, vec![c], Durability::Local).await;
        assert_eq!(s.ack().await.seq, seq);
    }
    let key = LogKey::new(ONE, A).unwrap();
    eventually("the tail is compacted", || core.store.tail_len(&key).unwrap() < 4).await;
    let got = replay(&redo, &stratum(A)).await.unwrap();
    assert!(got.iter().any(|e| e.from_snapshot), "{got:?}");
    assert!(got.len() < 10);
    assert_eq!(got.last().unwrap().seq, 10);
    assert_eq!(restore(&got), expected);
    // The snapshot's entries come first, all at its seq.
    let snap: Vec<_> = got.iter().take_while(|e| e.from_snapshot).collect();
    assert!(snap.iter().all(|e| e.seq == snap[0].seq && e.changes.len() == 1));
    s.send(11, vec![put("t.R", "z", b"11")], Durability::Local).await;
    assert_eq!(s.ack().await.seq, 11);
}

// ---------------------------------------------------------------------------
// The second site: REMOTE acks, shipping, catching up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn remote_is_acked_once_the_second_site_has_it() {
    let sites = TwoSites::start(1000);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Local).await;
    s.send(2, vec![put("p.Xid", "x", b"2")], Durability::Remote).await;
    assert_eq!(s.ack().await, ack(1, Durability::Local));
    assert_eq!(s.ack().await, ack(2, Durability::Local));
    assert_eq!(s.ack().await, ack(2, Durability::Remote));
    // The second site has the prefix, keyed by the source trust domain and tid.
    assert_eq!(seqs(&sites.copy(A)), vec![1, 2]);
    assert_eq!(sites.copy(A)[1].changes, vec![put("p.Xid", "x", b"2")]);
    assert!(sites.two.store.tenants(TWO).unwrap().is_empty(), "never keyed by the second site's own trust domain");
}

#[tokio::test]
async fn local_writes_are_shipped_behind_the_ack() {
    let sites = TwoSites::start(1000);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    for seq in 1..=20 {
        s.send(seq, vec![put("t.R", "x", &[seq as u8])], Durability::Local).await;
        assert_eq!(s.ack().await, ack(seq, Durability::Local));
    }
    eventually("the second site has all 20", || sites.copy_last_seq(A) == 20).await;
    no_ack_for(&mut s.acks, Duration::from_millis(50)).await;
}

#[tokio::test]
async fn shipping_catches_up_after_the_second_site_was_down() {
    let sites = TwoSites::start(1000);
    sites.link.set_up(false);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Local).await;
    s.send(2, vec![put("t.R", "y", b"2")], Durability::Remote).await;
    s.send(3, vec![put("t.R", "z", b"3")], Durability::Local).await;
    // Local acks do not wait for the second site.
    assert_eq!(s.ack().await, ack(1, Durability::Local));
    assert_eq!(s.ack().await, ack(2, Durability::Local));
    assert_eq!(s.ack().await, ack(3, Durability::Local));
    no_ack_for(&mut s.acks, Duration::from_millis(200)).await;
    assert_eq!(sites.copy_last_seq(A), 0);

    sites.link.set_up(true);
    assert_eq!(s.ack().await, ack(2, Durability::Remote));
    eventually("the second site caught up", || sites.copy_last_seq(A) == 3).await;

    // A connection lost mid-stream: the next REMOTE write still gets there.
    sites.link.cut();
    s.send(4, vec![put("t.R", "w", b"4")], Durability::Remote).await;
    assert_eq!(s.ack().await, ack(4, Durability::Local));
    assert_eq!(s.ack().await, ack(4, Durability::Remote));
    assert_eq!(restore(&sites.copy(A)), restore(&replay(&sites.redo(), &stratum(A)).await.unwrap()));
}

#[tokio::test]
async fn shipping_resumes_after_the_local_site_restarts() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let two = start_core(TWO, d2.path(), 1000, None);
    let link = InProcessSite::new(two.clone());
    link.set_up(false);
    let one = start_core(ONE, d1.path(), 1000, Some(link.clone()));
    let mut s = Appender::open(&RedoServer(one.clone()), &stratum(A)).await.unwrap();
    for seq in 1..=5 {
        s.send(seq, vec![put("t.R", "x", &[seq as u8])], Durability::Local).await;
        s.ack().await;
    }
    drop(s);
    one.shutdown().await;

    link.set_up(true);
    let _one = start_core(ONE, d1.path(), 1000, Some(link.clone()));
    let k = LogKey::new(ONE, A).unwrap();
    eventually("the restarted site shipped its backlog", || two.store.last_seq(&k).unwrap_or(0) == 5).await;
}

#[tokio::test]
async fn compaction_never_folds_what_the_second_site_does_not_have() {
    let sites = TwoSites::start(2);
    sites.link.set_up(false);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    for seq in 1..=6 {
        s.send(seq, vec![put("t.R", &format!("k{seq}"), b"v")], Durability::Local).await;
        s.ack().await;
    }
    let key = LogKey::new(ONE, A).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(sites.one.store.tail_len(&key).unwrap(), 6, "unshipped operations stay in the tail");

    sites.link.set_up(true);
    eventually("shipped", || sites.copy_last_seq(A) == 6).await;
    eventually("then compacted", || sites.one.store.tail_len(&key).unwrap() < 2).await;
    assert_eq!(seqs(&sites.copy(A)).last(), Some(&6));
    assert_eq!(restore(&sites.copy(A)).len(), 6);
}

// ---------------------------------------------------------------------------
// The receiving site
// ---------------------------------------------------------------------------

fn operation(td: &str, tid: &str, seq: u64) -> Shipment {
    Shipment {
        tenant: RESTORE.into(),
        source_trust_domain: td.into(),
        tid: tid.into(),
        body: Some(shipment::Body::Operation(ShippedOperation {
            seq,
            changes: vec![put("t.R", "x", &[seq as u8])],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn deletion(td: &str, tid: &str) -> Shipment {
    Shipment {
        tenant: RESTORE.into(),
        source_trust_domain: td.into(),
        tid: tid.into(),
        body: Some(shipment::Body::Deletion(ShippedDeletion::default())),
        ..Default::default()
    }
}

async fn ship(site: &SiteServer, shipments: Vec<Shipment>) -> Vec<Result<ShipmentAck, tonic::Status>> {
    let caller = service_caller(&format!("/tenants/{RESTORE}"), "redo");
    let inbound = Box::pin(tokio_stream::iter(shipments.into_iter().map(Ok)));
    let s = site.append(Some(&caller), tonic::Request::new(inbound)).await.unwrap().into_inner();
    tokio::time::timeout(Duration::from_secs(5), s.collect::<Vec<_>>()).await.unwrap()
}

fn site_ack(td: &str, tid: &str, seq: u64, deleted: bool) -> ShipmentAck {
    ShipmentAck { source_trust_domain: td.into(), tid: tid.into(), seq, deleted, ..Default::default() }
}

#[tokio::test]
async fn the_receiving_site_appends_in_order_and_acks_resends_and_gaps_with_what_it_holds() {
    let dir = tempfile::tempdir().unwrap();
    let site = SiteServer(start_core(TWO, dir.path(), 1000, None));
    let got = ship(&site, vec![operation(ONE, A, 1), operation(ONE, A, 2), operation(ONE, A, 2), operation(ONE, A, 4), operation(ONE, A, 3)]).await;
    let got: Vec<_> = got.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        got,
        vec![
            site_ack(ONE, A, 1, false),
            site_ack(ONE, A, 2, false),
            site_ack(ONE, A, 2, false), // a resend: acked, not written twice
            site_ack(ONE, A, 2, false), // past a gap: not written
            site_ack(ONE, A, 3, false),
        ]
    );
    let k = LogKey::new(ONE, A).unwrap();
    let copy: Vec<_> = site.0.store.replay(&k).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(seqs(&copy), vec![1, 2, 3]);
}

#[tokio::test]
async fn the_receiving_site_keys_by_trust_domain_and_refuses_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let site = SiteServer(start_core(TWO, dir.path(), 1000, None));
    // The same reserved tid from two deployments: two logs.
    let got = ship(&site, vec![operation(ONE, "00000001", 1), operation("three.test", "00000001", 1)]).await;
    assert!(got.iter().all(|a| a.is_ok()));
    assert_eq!(site.0.store.last_seq(&LogKey::new(ONE, "00000001").unwrap()).unwrap(), 1);
    assert_eq!(site.0.store.last_seq(&LogKey::new("three.test", "00000001").unwrap()).unwrap(), 1);

    let got = ship(&site, vec![operation(TWO, A, 1)]).await;
    assert_eq!(got[0].as_ref().unwrap_err().code(), Code::PermissionDenied);
    for bad in [operation(ONE, "acme", 1), operation("../x", A, 1)] {
        assert_eq!(ship(&site, vec![bad]).await[0].as_ref().unwrap_err().code(), Code::InvalidArgument);
    }
    let mut empty = operation(ONE, A, 1);
    empty.body = None;
    assert_eq!(ship(&site, vec![empty]).await[0].as_ref().unwrap_err().code(), Code::InvalidArgument);
}

#[tokio::test]
async fn the_receiving_site_deletes_idempotently_and_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let site = SiteServer(start_core(TWO, dir.path(), 1000, None));
    let got: Vec<_> = ship(&site, vec![operation(ONE, A, 1), deletion(ONE, A), deletion(ONE, A), operation(ONE, A, 2), deletion(ONE, B)])
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        got,
        vec![
            site_ack(ONE, A, 1, false),
            site_ack(ONE, A, 0, true),
            site_ack(ONE, A, 0, true),
            site_ack(ONE, A, 0, true), // a deleted tenant is never written again
            site_ack(ONE, B, 0, true), // a tenant never shipped is deleted all the same
        ]
    );
    assert!(site.0.store.is_deleted(&LogKey::new(ONE, A).unwrap()).unwrap());
}

// ---------------------------------------------------------------------------
// Tenant deletion (kernel's tombstone), at both sites
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_deleted_tenant_is_deleted_at_both_sites_and_refused() {
    let sites = TwoSites::start(1000);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    s.ack().await;
    s.ack().await;
    let mut b = Appender::open(&sites.redo(), &stratum(B)).await.unwrap();
    b.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    b.ack().await;
    b.ack().await;

    let live = TenantIdRow { tid: B.into(), tenant_namespace: format!("/tenants/{B}"), ..Default::default() };
    sites.one.apply_tenant_ids(vec![tombstone(A), live.clone()]);
    let a = LogKey::new(ONE, A).unwrap();
    assert!(sites.one.store.is_deleted(&a).unwrap(), "deleted here at once");
    eventually("deleted at the second site", || sites.two.store.is_deleted(&a).unwrap()).await;

    s.send(2, vec![put("t.R", "x", b"2")], Durability::Local).await;
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);
    assert_eq!(replay(&sites.redo(), &stratum(A)).await.unwrap_err().code(), Code::FailedPrecondition);
    // The live tenant is untouched, here and there.
    assert_eq!(replay(&sites.redo(), &stratum(B)).await.unwrap().len(), 1);
    assert_eq!(sites.copy_last_seq(B), 1);
    // Watch events repeat: deleting again is a no-op.
    sites.one.apply_tenant_ids(vec![tombstone(A), live]);
}

#[tokio::test]
async fn a_deletion_reaches_the_second_site_after_an_outage_and_a_restart() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let two = start_core(TWO, d2.path(), 1000, None);
    let link = InProcessSite::new(two.clone());
    let one = start_core(ONE, d1.path(), 1000, Some(link.clone()));
    let mut s = Appender::open(&RedoServer(one.clone()), &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    s.ack().await;
    s.ack().await;
    drop(s);

    link.set_up(false);
    one.apply_tenant_ids(vec![tombstone(A)]);
    one.shutdown().await;
    let a = LogKey::new(ONE, A).unwrap();
    assert!(!two.store.is_deleted(&a).unwrap());

    link.set_up(true);
    let _one = start_core(ONE, d1.path(), 1000, Some(link.clone()));
    eventually("the deletion was shipped", || two.store.is_deleted(&a).unwrap()).await;
}

#[tokio::test]
async fn a_remote_write_waiting_on_a_deleted_tenant_fails() {
    let sites = TwoSites::start(1000);
    sites.link.set_up(false);
    let mut s = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    s.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    assert_eq!(s.ack().await, ack(1, Durability::Local));
    sites.one.apply_tenant_ids(vec![tombstone(A)]);
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn tombstones_that_arrive_before_the_store_opens_are_applied() {
    // Kernel's watch may deliver before the first config opens the store.
    let dir = tempfile::tempdir().unwrap();
    let svc = crate::service::RedoService::new(ONE.into(), None);
    svc.on_tenant_ids(vec![tombstone(A)]);
    svc.configure(dir.path().into(), 1000).unwrap();
    let core = svc.core().unwrap();
    assert!(core.store.is_deleted(&LogKey::new(ONE, A).unwrap()).unwrap());
}

#[tokio::test]
async fn the_receiving_site_takes_shipments_only_from_the_restore_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let site = SiteServer(start_core(TWO, dir.path(), 1000, None));
    let inbound = || Box::pin(tokio_stream::iter(vec![Ok(operation(ONE, A, 1))]));
    for caller in [Some(service_caller(&format!("/tenants/{A}"), "redo")), Some(service_caller("/system", "redo")), None] {
        let r = site.append(caller.as_ref(), tonic::Request::new(inbound())).await;
        assert_eq!(r.err().unwrap().code(), Code::PermissionDenied);
    }
}

#[tokio::test]
async fn one_tenant_the_second_site_cannot_take_does_not_hold_up_the_others() {
    let sites = TwoSites::start(1000);
    // Site two's copy of A is damaged: every shipment of A fails there.
    let copy = sites._dirs.1.path().join(ONE).join(A);
    std::fs::create_dir_all(&copy).unwrap();
    std::fs::write(copy.join("tail"), [0x5au8; 64]).unwrap();
    let mut a = Appender::open(&sites.redo(), &stratum(A)).await.unwrap();
    a.send(1, vec![put("t.R", "x", b"1")], Durability::Local).await;
    a.ack().await;
    // A has failed there by now, and keeps failing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = Appender::open(&sites.redo(), &stratum(B)).await.unwrap();
    b.send(1, vec![put("t.R", "x", b"1")], Durability::Remote).await;
    assert_eq!(b.ack().await, ack(1, Durability::Local));
    assert_eq!(b.ack().await, ack(1, Durability::Remote));
}

#[tokio::test]
async fn remote_fails_rather_than_waits_when_the_second_site_can_never_have_it() {
    // Compacted while no second site was configured: the second site can no
    // longer receive this tenant's log from its start.
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let one = start_core(ONE, d1.path(), 2, None);
    let mut s = Appender::open(&RedoServer(one.clone()), &stratum(A)).await.unwrap();
    for seq in 1..=4 {
        s.send(seq, vec![put("t.R", "x", &[seq as u8])], Durability::Local).await;
        s.ack().await;
    }
    let k = LogKey::new(ONE, A).unwrap();
    eventually("compacted", || one.store.tail_len(&k).unwrap() < 3).await;
    drop(s);
    one.shutdown().await;

    let two = start_core(TWO, d2.path(), 1000, None);
    let one = start_core(ONE, d1.path(), 2, Some(InProcessSite::new(two)));
    let mut s = Appender::open(&RedoServer(one), &stratum(A)).await.unwrap();
    s.send(5, vec![put("t.R", "y", b"5")], Durability::Remote).await;
    assert_eq!(s.ack().await, ack(5, Durability::Local));
    assert_eq!(s.error().await.code(), Code::FailedPrecondition);
}
