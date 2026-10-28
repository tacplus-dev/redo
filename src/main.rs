//! redo: durability for every tenant's Stratum, and its copy at a second site.
//! The design is in proto/services/redo.proto and src/service.rs.

mod service;
mod ship;
mod site;
mod store;
#[cfg(test)]
mod service_tests;
#[cfg(test)]
mod test_support;

use std::error::Error;
use std::sync::Arc;

use redo_model::s2sm::handler::Handler;
use redo_model::s2sm::internal::grpc::servers::services::redo::site::RedoSiteHandler;
use redo_model::s2sm::internal::grpc::servers::sys::redolog::redo::RedoHandler;
use redo_model::s2sm::ServiceManager;
use redo_model::types::services::redo::redo::Config;

use service::{LazyRedo, LazySite, RedoService};

#[tokio::main]
async fn main() {
    // The chassis logger: it redacts.
    if let Err(e) = logger::install_bootstrap() {
        eprintln!("FATAL: could not install the redacting logger: {e}");
        std::process::exit(1);
    }
    if let Err(e) = run().await {
        tracing::error!(error = %e, "redo failed");
        let mut source = e.source();
        while let Some(cause) = source {
            tracing::error!(cause = %cause, "caused by");
            source = cause.source();
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    // ServiceManager::new() reads BOOTSTRAP_CONFIG, connects to kernel over
    // mTLS, and returns once the service has an identity and config.
    let sm = Arc::new(ServiceManager::new().await?);

    // No second site yet: the deployed link is a generated cross-deployment S2S
    // client (tacplus-dev/tacplus#204), so REMOTE writes are refused until then.
    let svc = RedoService::new(sm.config().spiffe_trust_domain.clone(), None);

    // Tenant deletion: kernel tombstones a deleted tenant's tid. The cache is
    // read once too, for a snapshot that arrived before the callback was
    // registered; tombstones before the store opens wait for it.
    let config_state = sm.state_clients().services().config_state();
    let on_ids = svc.clone();
    config_state.on_tenant_ids_change(Arc::new(move |rows| on_ids.on_tenant_ids(rows.values().cloned().collect())));
    svc.on_tenant_ids(config_state.get_tenant_ids());

    sm.run(Arc::new(RedoHandlerImpl(svc))).await
}

struct RedoHandlerImpl(Arc<RedoService>);

#[async_trait::async_trait]
impl Handler for RedoHandlerImpl {
    fn redo_site_grpc_impl(&self) -> Arc<dyn RedoSiteHandler> {
        Arc::new(LazySite(self.0.clone()))
    }

    fn redo_grpc_impl(&self) -> Arc<dyn RedoHandler> {
        Arc::new(LazyRedo(self.0.clone()))
    }

    /// The first config opens the store, before the server serves.
    async fn redo_site_config(&self, cfg: &Config) -> Result<(), Box<dyn Error>> {
        self.0.configure(cfg.data_dir.clone().into(), cfg.compact_after.max(1) as u64)?;
        Ok(())
    }

    async fn shutdown(&self) {
        self.0.shutdown().await;
    }
}
