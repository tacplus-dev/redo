//! Shipping this site's logs to the second site, behind the local ack.
//!
//! One `RedoSite.Append` stream at a time. A tenant with something to ship (an
//! append, a deletion, or anything left at a start) is queued; the shipper sends
//! its operations after the last seq the second site acked, in batches, and
//! records each ack (`Core::set_remote`, which releases the REMOTE acks waiting
//! on it, and the store's `shipped`). When the second site cannot be reached, or
//! the stream breaks, the shipper waits and opens another, and goes on from
//! what was acked: catching up after an outage is the same path as shipping.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use redo_model::types::services::redo::redo::{shipment, Shipment, ShipmentAck, ShippedDeletion};
use tokio::sync::{mpsc, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Status;

use crate::service::{Core, RESTORE_TID};
use crate::site::{SecondSite, ShipmentAcks};
use crate::store::LogKey;

/// Operations sent before waiting for their acks.
const BATCH: usize = 256;


/// The tenants with something to ship, each once, taken in turn.
#[derive(Default)]
pub struct ShipQueue {
    pending: Mutex<(VecDeque<LogKey>, HashSet<LogKey>)>,
    wake: Notify,
}

impl ShipQueue {
    pub fn mark(&self, key: LogKey) {
        let mut p = self.pending.lock().unwrap();
        if p.1.insert(key.clone()) {
            p.0.push_back(key);
        }
        drop(p);
        self.wake.notify_one();
    }

    fn pop(&self) -> Option<LogKey> {
        let mut p = self.pending.lock().unwrap();
        let key = p.0.pop_front()?;
        p.1.remove(&key);
        Some(key)
    }
}

/// Why a tenant's shipping stopped.
enum ShipError {
    /// The stream broke: open another, and go on.
    Stream(Status),
    /// This tenant cannot be shipped now (its log here, or its copy there):
    /// it waits a while, and the others go on. `stream_ended` when the second
    /// site ended the stream over it.
    Tenant { status: Status, stream_ended: bool },
}

/// Ships until the core shuts down.
pub(crate) async fn run(core: Arc<Core>, site: Arc<dyn SecondSite>, retry_min: Duration, retry_max: Duration) {
    let retry_tenant = retry_max;
    let queue = core.ship_queue.clone().expect("shipping without a second site");
    match core.store.tenants(&core.trust_domain) {
        Ok(keys) => keys.into_iter().for_each(|k| queue.mark(k)),
        Err(e) => tracing::error!(error = %e, "listing this site's logs to ship"),
    }
    let mut retry = retry_min;
    loop {
        let (tx, rx) = mpsc::channel(BATCH);
        match site.open(Box::pin(ReceiverStream::new(rx))).await {
            Ok(mut acks) => {
                retry = retry_min;
                let e = ship(&core, &queue, &tx, &mut acks, retry_tenant).await;
                tracing::warn!(error = %e, "the stream to the second site broke; shipping goes on from what it acked");
                // A stream ended over one tenant, which now waits: reopen at once.
                if e.code() == tonic::Code::Aborted {
                    continue;
                }
            }
            Err(e) => tracing::warn!(error = %e, retry_in = ?retry, "the second site cannot be reached"),
        }
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(retry_max);
    }
}

/// Ships queued tenants over one stream until it breaks.
async fn ship(core: &Arc<Core>, queue: &Arc<ShipQueue>, tx: &mpsc::Sender<Shipment>, acks: &mut ShipmentAcks, retry_tenant: Duration) -> Status {
    loop {
        let notified = queue.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let Some(key) = queue.pop() else {
            notified.await;
            continue;
        };
        match ship_tenant(core, &key, tx, acks).await {
            Ok(true) => queue.mark(key),
            Ok(false) => {}
            Err(ShipError::Stream(e)) => {
                queue.mark(key);
                return e;
            }
            Err(ShipError::Tenant { status, stream_ended }) => {
                tracing::error!(tenant = %key, error = %status, retry_in = ?retry_tenant, "this tenant cannot be shipped now; the others go on");
                let q = queue.clone();
                let key_for_error = key.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(retry_tenant).await;
                    q.mark(key);
                });
                if stream_ended {
                    return Status::aborted(format!("the second site ended the stream over {key_for_error}: {status}"));
                }
            }
        }
    }
}

/// Ships one batch of a tenant's log, or its deletion. Returns whether there
/// is more to ship.
async fn ship_tenant(core: &Arc<Core>, key: &LogKey, tx: &mpsc::Sender<Shipment>, acks: &mut ShipmentAcks) -> Result<bool, ShipError> {
    let local = |status: Status| ShipError::Tenant { status, stream_ended: false };
    let store = core.store.clone();
    let k = key.clone();
    let work = tokio::task::spawn_blocking(move || -> Result<Work, crate::store::StoreError> {
        if store.deletion_unshipped(&k)? {
            return Ok(Work::Deletion);
        }
        if store.is_deleted(&k)? {
            return Ok(Work::None);
        }
        let shipped = store.shipped(&k)?;
        Ok(Work::Operations(shipped, store.read_tail(&k, shipped, BATCH)?))
    })
    .await
    .map_err(|e| local(Status::internal(e.to_string())))?
    .map_err(|e| local(Status::internal(format!("{key}: {e}"))))?;

    let shipment = |body| Shipment {
        tenant: RESTORE_TID.into(),
        source_trust_domain: key.trust_domain.clone(),
        tid: key.tid.clone(),
        body: Some(body),
        ..Default::default()
    };
    match work {
        Work::None => Ok(false),
        Work::Deletion => {
            send(tx, shipment(shipment::Body::Deletion(ShippedDeletion::default()))).await?;
            let a = next_ack(key, acks).await?;
            if !a.deleted {
                return Err(ShipError::Stream(Status::internal(format!("{key}: the second site answered a deletion with seq {}", a.seq))));
            }
            let (store, k) = (core.store.clone(), key.clone());
            tokio::task::spawn_blocking(move || store.set_deletion_shipped(&k))
                .await
                .map_err(|e| local(Status::internal(e.to_string())))?
                .map_err(|e| local(Status::internal(format!("{key}: {e}"))))?;
            tracing::info!(tenant = %key, "the second site deleted the tenant's log");
            Ok(false)
        }
        Work::Operations(shipped, ops) => {
            let Some(first) = ops.first() else { return Ok(false) };
            if first.seq != shipped + 1 {
                // Only an operation the second site acked is ever folded, so this
                // is a second site that lost what it acked. It needs this tenant's
                // snapshot, which RedoSite does not carry (PROPOSALS.md).
                tracing::error!(tenant = %key, shipped, first = first.seq, "the second site lacks operations already folded into the snapshot; not shipping this tenant");
                core.set_remote_failed(key, "the second site lacks operations this site already compacted, so it cannot have this tenant's log");
                return Ok(false);
            }
            let (n, last) = (ops.len(), ops.last().map(|o| o.seq).unwrap_or(0));
            for op in ops {
                send(tx, shipment(shipment::Body::Operation(op))).await?;
            }
            let mut held = shipped;
            for _ in 0..n {
                let a = next_ack(key, acks).await?;
                if a.deleted {
                    tracing::error!(tenant = %key, "the second site has deleted a tenant this site has not; not shipping it");
                    core.set_remote_failed(key, "the second site has this tenant deleted");
                    return Ok(false);
                }
                held = a.seq;
                core.set_remote(key, held);
            }
            let (store, k) = (core.store.clone(), key.clone());
            tokio::task::spawn_blocking(move || store.set_shipped(&k, held))
                .await
                .map_err(|e| local(Status::internal(e.to_string())))?
                .map_err(|e| local(Status::internal(format!("{key}: {e}"))))?;
            core.maybe_compact(key);
            // Past a gap the second site acked less than was sent: resend from there.
            Ok(held < last || n == BATCH)
        }
    }
}

enum Work {
    None,
    Deletion,
    /// The last seq the second site acked, and the operations after it.
    Operations(u64, Vec<redo_model::types::services::redo::redo::ShippedOperation>),
}

async fn send(tx: &mpsc::Sender<Shipment>, s: Shipment) -> Result<(), ShipError> {
    tx.send(s)
        .await
        .map_err(|_| ShipError::Stream(Status::unavailable("the stream to the second site is closed")))
}

/// The next ack, which must be for `key`: acks come in the order shipments
/// went. An error the second site ends the stream with is over this tenant's
/// shipment, unless it is the connection's.
async fn next_ack(key: &LogKey, acks: &mut ShipmentAcks) -> Result<ShipmentAck, ShipError> {
    let a = match acks.next().await {
        None => return Err(ShipError::Stream(Status::unavailable("the second site ended the stream"))),
        Some(Err(s)) if matches!(s.code(), tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::DeadlineExceeded | tonic::Code::Unknown) => {
            return Err(ShipError::Stream(s))
        }
        Some(Err(status)) => return Err(ShipError::Tenant { status, stream_ended: true }),
        Some(Ok(a)) => a,
    };
    if a.source_trust_domain != key.trust_domain || a.tid != key.tid {
        return Err(ShipError::Stream(Status::internal(format!("an ack for {}/{} while waiting for {key}", a.source_trust_domain, a.tid))));
    }
    Ok(a)
}
