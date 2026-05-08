use std::sync::Arc;

use protogen::metastore::strategy::ResolveErrorStrategy;
use protogen::metastore::types::catalog::CatalogState;
use protogen::metastore::types::service::Mutation;
use tracing::debug;

use super::client::MetastoreClientHandle;
use crate::errors::{CatalogError, Result};

/// Wrapper around a metastore client for mutating the catalog.
#[derive(Clone)]
pub struct CatalogMutator {
    pub client: Option<MetastoreClientHandle>,
}

impl CatalogMutator {
    pub fn empty() -> Self {
        CatalogMutator { client: None }
    }

    pub fn is_empty(&self) -> bool {
        self.client.is_none()
    }
    pub fn new(client: Option<MetastoreClientHandle>) -> Self {
        CatalogMutator { client }
    }

    pub fn get_metastore_client(&self) -> Option<&MetastoreClientHandle> {
        self.client.as_ref()
    }

    /// Commit the catalog state.
    /// This persists the state to the metastore.
    /// The `current_catalog_version` is the version of the catalog prior to the state being committed.
    /// the 'state.version' should always be greater than 'current_catalog_version'.
    /// If not, the commit will not succeed.
    pub async fn commit_state(
        &self,
        current_catalog_version: u64,
        state: CatalogState,
    ) -> Result<Arc<CatalogState>> {
        let client = match &self.client {
            Some(client) => client,
            None => return Err(CatalogError::new("metastore client not configured")),
        };

        client
            .commit_state(current_catalog_version, state.clone())
            .await
    }

    /// Mutate the catalog if possible.
    /// This returns the catalog state with the mutations reflected.
    /// IMPORTANT: these changes are not yet persisted and must be 'committed' manually via `commit_state`.
    /// If you wish to mutate and immediately commit, use `mutate_and_commit`
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date catalog.
    pub async fn mutate(
        &self,
        catalog_version: u64,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let client = match &self.client {
            Some(client) => client,
            None => return Err(CatalogError::new("metastore client not configured")),
        };

        // Note that when we have transactions, these shouldn't be sent until
        // commit.
        let mutations: Vec<_> = mutations.into_iter().collect();
        let state = match client.try_mutate(catalog_version, mutations.clone()).await {
            Ok(state) => state,
            Err(CatalogError {
                msg,
                strategy: Some(ResolveErrorStrategy::FetchCatalogAndRetry),
            }) => {
                // Go ahead and refetch the catalog and retry the mutation.
                //
                // Note that this relies on metastore _always_ being stricter
                // when validating mutations. What this means is that retrying
                // here should be semantically equivalent to manually refreshing
                // the catalog and rerunning and replanning the query.
                debug!(error_message = msg, "retrying mutations");

                client.refresh_cached_state().await?;
                let state = client.get_cached_state().await?;
                let version = state.version;

                client.try_mutate(version, mutations).await?
            }
            Err(e) => return Err(e),
        };

        Ok(state)
    }

    /// Mutate the catalog if possible and immediately commit the changes.
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date
    /// catalog. The retry is bounded so concurrent writers can't loop
    /// forever on a hot key.
    pub async fn mutate_and_commit(
        &self,
        catalog_version: u64,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let mutations: Vec<_> = mutations.into_iter().collect();
        let mut version = catalog_version;
        let mut attempt = 0u32;
        const MAX_ATTEMPTS: u32 = 5;
        loop {
            // `mutate` itself retries on `try_mutate` rejection (FetchCatalogAndRetry),
            // so by the time we reach commit_state the in-memory state is rebased
            // on the freshest catalog version `mutate` saw. The race we're
            // catching here is when *another* session commits AFTER our `mutate`
            // but BEFORE our `commit_state` lands — the commit_state then fails
            // with stale-version. We re-fetch and replay from the top.
            let state = self.mutate(version, mutations.clone()).await?;
            match self
                .commit_state(version, state.as_ref().clone())
                .await
            {
                Ok(state) => return Ok(state),
                Err(CatalogError {
                    msg,
                    strategy: Some(ResolveErrorStrategy::FetchCatalogAndRetry),
                }) if attempt + 1 < MAX_ATTEMPTS => {
                    attempt += 1;
                    debug!(
                        attempt,
                        error_message = msg,
                        "commit_state lost a race; refreshing catalog and replaying mutations"
                    );
                    let client = self.client.as_ref().ok_or_else(|| {
                        CatalogError::new("metastore client not configured")
                    })?;
                    client.refresh_cached_state().await?;
                    let fresh = client.get_cached_state().await?;
                    version = fresh.version;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl From<MetastoreClientHandle> for CatalogMutator {
    fn from(value: MetastoreClientHandle) -> Self {
        CatalogMutator {
            client: Some(value),
        }
    }
}
