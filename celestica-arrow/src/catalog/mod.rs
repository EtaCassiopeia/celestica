use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::catalog::{CatalogList, CatalogProvider};

/// Catalog lists holds multiple catalogs. Each context has a single catalog list.
struct InMemoryCatalogList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl InMemoryCatalogList {
    fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}

impl CatalogList for InMemoryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.write().unwrap();
        catalogs.insert(name, catalog.clone());
        Some(catalog)
    }

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.keys().cloned().collect()
    }

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(name).cloned()
    }
}
