//! The second site, behind one interface.
//!
//! The local redo ships each tenant's log to the redo of another deployment
//! over `redo.RedoSite.Append` (proto/services/redo.proto). The deployed link
//! between the two is a generated cross-deployment S2S client
//! (tacplus-dev/tacplus#204); until it lands no deployment configures a second
//! site, and the tests reach a second in-process site through this same
//! interface (test_support.rs).

use std::pin::Pin;

use redo_model::types::services::redo::redo::{Shipment, ShipmentAck};
use tokio_stream::Stream;

pub type Shipments = Pin<Box<dyn Stream<Item = Shipment> + Send>>;
pub type ShipmentAcks = Pin<Box<dyn Stream<Item = Result<ShipmentAck, tonic::Status>> + Send>>;

/// The redo at the second site.
#[async_trait::async_trait]
pub trait SecondSite: Send + Sync + 'static {
    /// Opens a `RedoSite.Append` stream: `shipments` go out, their acks come
    /// back in the same order. The stream ending, or an error on it, is the
    /// site becoming unreachable; the shipper opens another.
    async fn open(&self, shipments: Shipments) -> Result<ShipmentAcks, tonic::Status>;
}
