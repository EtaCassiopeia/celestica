use std::path::Path;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::provider::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use tokio::sync::Mutex;

use crate::mem_table::{CelesMemTable, TableFlusher};

/// ```ignore
/// let runtime_env = create_runtime_env()?;
///
/// let mut state = SessionState::with_config_rt(session_config.clone(), Arc::new(runtime_env));
/// let mut table_factories = state.table_factories_mut();
/// table_factories.insert("LANCE".into(), Arc::new(LanceListingTableFactory::new()));
/// let mut ctx= SessionContext::with_state(state);
/// ```

pub struct LanceListingTableFactory {}

impl LanceListingTableFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for LanceListingTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for LanceListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let file_type_upper = cmd.file_type.as_str().to_uppercase();
        if file_type_upper != "LANCE" {
            return Err(DataFusionError::Execution(format!(
                "Unknown FileType {}",
                cmd.file_type
            )));
        }

        // let file_extension = get_extension(cmd.location.as_str());
        //
        // let file_format: Arc<dyn FileFormat> = Arc::new(LanceFormat);
        println!("LanceListingTableFactory::create");
        let catalog_names = state.catalog_list().catalog_names();
        for catalog_name in catalog_names {
            println!("catalog_name: {}", catalog_name);
            let catalog = state.catalog_list().catalog(&catalog_name).unwrap();
            let schema_names = catalog.schema_names();
            for schema_name in schema_names {
                println!("schema_name: {}", schema_name);
                let schema = catalog.schema(&schema_name).unwrap();
                schema.table_names().iter().for_each(|table_name| {
                    println!("table_name: {}", table_name);
                    async {
                        let table = schema.table(table_name).await.unwrap();
                        println!("table schema: {:?}", table.schema());
                    };
                });
            }
        }

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());

        println!("schema: {:?}", schema);

        //TODO fix me

        let table_path = ListingTableUrl::parse(&cmd.location)?;

        // look for 'infinite' as an option
        // let infinite_source = cmd.unbounded;
        // let infinite_source = true;
        // let options = ListingOptions::new(file_format)
        //     .with_collect_stat(state.config().collect_statistics())
        //     .with_file_extension(file_extension)
        //     .with_infinite_source(infinite_source)
        //     .with_file_sort_order(Some(cmd.order_exprs.clone()));
        //
        // let resolved_schema = match provided_schema {
        //     None => options.infer_schema(state, &table_path).await?,
        //     Some(s) => s,
        // };
        // let config = ListingTableConfig::new(table_path)
        //     .with_listing_options(options)
        //     .with_schema(resolved_schema);
        //
        // let table =
        //     ListingTable::try_new(config)?.with_definition(cmd.definition.clone());

        println!("table_path: {:?}, name: {:?}", table_path, cmd.name.table());
        let table_flusher = Arc::new(Mutex::new(TableFlusher::new(
            table_path.to_string(),
            String::from(cmd.name.table()),
        )));

        let table = CelesMemTable::try_new(schema, table_flusher).map_err(|e| {
            DataFusionError::Execution(format!(
                "Error creating table provider: {}",
                e.to_string()
            ))
        })?;

        Ok(Arc::new(table))
    }
}

// Get file extension from path
fn get_extension(path: &str) -> String {
    let res = Path::new(path).extension().and_then(|ext| ext.to_str());
    match res {
        Some(ext) => format!(".{ext}"),
        None => "".to_string(),
    }
}
