use std::any::Any;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Statistics;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};

#[derive(Default, Debug)]
pub struct LanceFormat;

#[async_trait]
impl FileFormat for LanceFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        println!("LanceFormat::infer_schema");

        // let catalog = state.catalog_list().catalog("default").unwrap();

        // let mut schemas = Vec::with_capacity(objects.len());
        // for object in objects {
        //     let schema =
        //         fetch_schema(store.as_ref(), object, self.metadata_size_hint).await?;
        //     schemas.push(schema)
        // }
        //
        // let schema = if self.skip_metadata(state.config_options()) {
        //     Schema::try_merge(clear_metadata(schemas))
        // } else {
        //     Schema::try_merge(schemas)
        // }?;
        //
        // Ok(Arc::new(schema))

        //TODO fix me
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        Ok(schema)
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
        // TODO get partitions from store
        // let mut partitions = vec![];
        // // for arc_inner_vec in self.partitions.iter() {
        // //     let inner_vec = arc_inner_vec.read().await;
        // //     partitions.push(inner_vec.clone())
        // // }
        //
        //
        // // TODO: Infer schema from MemTable or any other source
        // let field_id = Field::new("id", DataType::Int64, false);
        // let field_name = Field::new("name", DataType::Utf8, false);
        //
        // let schema = SchemaRef::new(Schema::new(vec![field_id,field_name]));
        //
        // Ok(Arc::new(MemoryExec::try_new(
        //     &partitions,
        //     schema,
        //     None,
        // )?))
    }
}
