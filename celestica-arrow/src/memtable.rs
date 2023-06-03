use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::memory::PartitionData;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::stream::{Stream, TryStreamExt};
use futures::StreamExt;
use tokio::sync::RwLock;

pub struct CelesMemTable {
    schema: SchemaRef,
    pub(crate) batches: Vec<PartitionData>,
}

impl CelesMemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            schema,
            // A single partition with no data
            batches: vec![Arc::new(RwLock::new(vec![]))],
        })
    }

    async fn write_all(&self, mut data: SendableRecordBatchStream) -> Result<u64> {
        // determine the number of partitions, currently this is fixed to one partition
        let num_partitions = self.batches.len();

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        // write the outputs into the batches
        for (target, mut batches) in self.batches.iter().zip(new_batches.into_iter()) {
            // Append all the new batches in one go to minimize locking overhead
            target.write().await.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}

impl Debug for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl Display for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let partition_count = self.batches.len();
        write!(f, "MemoryTable (partitions={partition_count})")
    }
}

#[async_trait]
impl TableProvider for CelesMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in self.batches.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone())
        }

        Ok(Arc::new(MemoryExec::try_new_owned_data(
            partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }
}

#[cfg(test)]
mod tests {}
