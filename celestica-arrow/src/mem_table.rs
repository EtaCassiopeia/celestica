use std::any::Any;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{
    ExecutionPlan,
    RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, TryStreamExt};
use futures::StreamExt;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::WriteMode;
use lancedb::error::Error as LanceError;
use lancedb::table::Table;
use log::{debug, info, log};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::{Mutex, RwLock};

use crate::errors::LanceErrorWrapper;
use crate::fs::create_if_not_exists;

pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

pub struct MemTableOptions {
    /// The number of partitions to create
    pub(crate) partitions: usize,
    /// The maximum number of records before flushing a partition
    pub(crate) max_rows: usize,
}

impl Default for MemTableOptions {
    fn default() -> Self {
        Self {
            partitions: 1,
            max_rows: 10000,
        }
    }
}

impl MemTableOptions {
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = max_rows;
        self
    }
}

impl Display for MemTableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MemTableOptions {{ partitions: {}, max_records: {} }}",
            self.partitions, self.max_rows
        )
    }
}

#[async_trait(?Send)]
pub trait Flushable: Send {
    async fn flush(
        &mut self,
        partitions: &Vec<PartitionData>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct TableFlusher {
    base_url: String,
    table: String,
}

impl TableFlusher {
    pub fn new(base_url: String, table: String) -> Self {
        Self { base_url, table }
    }
}

#[async_trait(?Send)]
impl Flushable for TableFlusher {
    async fn flush(
        &mut self,
        partitions: &Vec<PartitionData>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create the the base directory to store data if it doesn't exist.
        create_if_not_exists(&self.base_url).await?;

        // Acquire the lock.
        let partition_data: Vec<RecordBatch> = partitions[0].read().await.clone();
        let buffer = Box::new(RecordBatchBuffer::new(partition_data));

        // Open the table, or create it if it doesn't exist.
        match Table::open(&self.base_url, &self.table).await {
            Ok(mut table) => {
                debug!(
                    "Flushing {} rows to table {}, total rows {}",
                    buffer.num_rows(),
                    self.table,
                    table.count_rows().await.unwrap()
                );

                table.add(buffer, Some(WriteMode::Append)).await.map(|_| ())
            },
            Err(LanceError::TableNotFound { .. }) => {
                Table::create(&self.base_url, &self.table, buffer)
                    .await
                    .map(|_| ())
            },
            Err(e) => Err(e),
        }
        .map_err(|e| LanceErrorWrapper::from(e).into())
    }
}

pub struct CelesMemTable {
    schema: SchemaRef,
    pub(crate) partitions: Vec<PartitionData>,
    flusher: Arc<Mutex<dyn Flushable>>,
    options: MemTableOptions,
}

impl CelesMemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(
        schema: SchemaRef,
        flusher: Arc<Mutex<dyn Flushable>>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            // A single partition with no data
            partitions: vec![Arc::new(RwLock::new(vec![]))],
            flusher,
            options: MemTableOptions::default(),
        })
    }

    pub fn try_new_with_options(
        schema: SchemaRef,
        flusher: Arc<Mutex<dyn Flushable>>,
        options: MemTableOptions,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            // A single partition with no data
            partitions: vec![Arc::new(RwLock::new(vec![]))],
            flusher,
            options,
        })
    }

    pub fn with_options(mut self, options: MemTableOptions) -> Self {
        self.options = options;
        self
    }

    async fn write_all(&mut self, mut data: SendableRecordBatchStream) -> Result<u64> {
        // determine the number of partitions, currently this is fixed to one partition
        let num_partitions = self.partitions.len();

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
        for (target, mut batches) in self.partitions.iter().zip(new_batches.into_iter())
        {
            // Append all the new batches in one go to minimize locking overhead
            // Retaining the write lock prevents any flushes from occurring amidst the append calls
            target.write().await.append(&mut batches);
        }

        let should_flush = self.should_flush().await;
        if should_flush {
            self.flush().await?;
        }

        Ok(row_count as u64)
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<u64> {
        //TODO: implement this
        //let mut stream = self.scan(ctx, None, &[], None).await?;
        let row_count = 10;
        Ok(row_count)
    }

    async fn should_flush(&self) -> bool {
        //TODO: For now, we only check the first partition
        let partition_guard = self.partitions[0].read().await;
        let partition = &*partition_guard;
        partition.iter().map(|v| v.num_rows()).sum::<usize>() >= self.options.max_rows
    }

    async fn flush(&mut self) -> Result<()> {
        info!("Flushing memtable");
        let mut flusher = self.flusher.lock().await;
        let partitions = self.partitions.clone();
        flusher.flush(&partitions).await?;

        //TODO: For now, we only clear the first partition
        let mut partition = self.partitions[0].write().await;
        partition.clear();
        Ok(())
    }
}

impl Debug for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTable")
            .field("num_partitions", &self.partitions.len())
            .finish()
    }
}

impl Display for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let partition_count = self.partitions.len();
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

    // fn get_logical_plan(&self) -> Option<&LogicalPlan> {
    //     todo!()
    // }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in self.partitions.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone())
        }

        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }

    // fn statistics(&self) -> Option<Statistics> {
    //     todo!()
    // }

    // async fn insert_into(
    //     &self,
    //     _state: &SessionState,
    //     _input: &LogicalPlan,
    // ) -> Result<()> {
    //     todo!()
    // }
}

pub struct MemTableStream {
    schema: SchemaRef,
    receiver: Receiver<RecordBatch>,
}

impl MemTableStream {
    pub fn new(schema: SchemaRef, receiver: Receiver<RecordBatch>) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for MemTableStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for MemTableStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use arrow_array::{StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use lancedb::table::Table;
    use rand::Rng;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;

    use super::*;

    struct NoOpFlusher {}

    impl NoOpFlusher {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait(?Send)]
    impl Flushable for NoOpFlusher {
        async fn flush(
            &mut self,
            _partitions: &Vec<PartitionData>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }
    }

    fn tempdir() -> std::io::Result<TempDir> {
        TempDir::new()
    }

    fn get_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("bank_account", DataType::UInt64, true),
        ]))
    }

    fn generate_account_number() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen_range(1_000_000_000u64..10_000_000_000u64)
    }

    fn stream_data(
        schema_ref: SchemaRef,
        sender: Sender<RecordBatch>,
        number_of_records: i64,
    ) -> Sender<()> {
        let (write_all_called_sender, mut write_all_called_receiver) = channel::<()>(1);

        tokio::spawn(async move {
            // Wait for the signal that write_all has been called
            write_all_called_receiver.recv().await;

            for i in 0..number_of_records {
                // Create your RecordBatch here
                let record_batch: RecordBatch = RecordBatch::try_new(
                    Arc::clone(&schema_ref),
                    vec![
                        Arc::new(StringArray::from(vec![format!("id-{}", i)])),
                        Arc::new(UInt64Array::from(vec![generate_account_number()])),
                    ],
                )
                .unwrap();

                // Send the RecordBatch to the receiver
                if sender.send(record_batch).await.is_err() {
                    debug!("Receiver dropped");
                    return;
                }
            }
            // This will close the channel and make .recv() on the receiver end return None.
            // This is not strictly necessary, as the sender will be dropped automatically at the end of the scope.
            drop(sender);
        });

        write_all_called_sender
    }

    #[tokio::test]
    async fn insert() {
        let schema_ref: SchemaRef = get_schema();

        let no_op_flusher = Arc::new(Mutex::new(NoOpFlusher::new()));

        let mut mem_table =
            CelesMemTable::try_new(Arc::clone(&schema_ref), no_op_flusher).unwrap();

        let (sender, receiver) = channel(100);
        let initiated_stream = stream_data(schema_ref.clone(), sender, 10i64);

        // Create a stream for RecordBatch that will funnel data into the MemTable
        let stream: SendableRecordBatchStream =
            Box::pin(MemTableStream::new(Arc::clone(&schema_ref), receiver));

        // Kick off the stream to begin data transmission
        initiated_stream.send(()).await.unwrap();

        let row_count = mem_table.write_all(stream).await.unwrap();

        assert_eq!(row_count, 10, "Expected to write 10 rows");
        assert_eq!(
            mem_table.partitions.len(),
            1,
            "Expected to have 1 partition"
        );
        assert_eq!(
            mem_table.partitions[0].read().await.len(),
            10,
            "Expected to have 10 batch in partition"
        );

        for i in 0..10 {
            assert_eq!(
                mem_table.partitions[0].read().await[i]
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                format!("id-{}", i)
            );
        }
    }

    #[tokio::test]
    async fn flush() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let table_name = String::from("test");

        let table_flusher = Arc::new(Mutex::new(TableFlusher::new(
            uri.to_string(),
            table_name.clone(),
        )));

        let schema_ref: SchemaRef = get_schema();

        let memtable_options = MemTableOptions::default().with_max_rows(10);

        let mut mem_table = CelesMemTable::try_new_with_options(
            Arc::clone(&schema_ref),
            table_flusher,
            memtable_options,
        )
        .unwrap();

        let (sender, receiver) = channel(100);
        // Sending 15 records to the MemTable, since MemTable has a capacity of 10, this should trigger a flush
        let initiated_stream = stream_data(schema_ref.clone(), sender, 15i64);

        // Create a stream for RecordBatch that will funnel data into the MemTable
        let stream: SendableRecordBatchStream =
            Box::pin(MemTableStream::new(Arc::clone(&schema_ref), receiver));

        // Kick off the stream to begin data transmission
        initiated_stream.send(()).await.unwrap();

        let row_count = mem_table.write_all(stream).await.unwrap();
        assert_eq!(row_count, 15, "Expected to write 15 rows");

        let mut retries = 5;

        while retries > 0 {
            match Table::open(uri, &table_name).await {
                Ok(table) => {
                    let written_rows = table.count_rows().await.unwrap();
                    assert_eq!(written_rows, 15, "Expected to write 15 rows");
                    break;
                },
                Err(err) => {
                    if retries == 1 {
                        panic!("Failed to open table after several attempts: {}", err);
                    }
                    retries -= 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
            }
        }

        assert_eq!(
            mem_table.partitions[0].read().await.len(),
            0,
            "Expected to have an empty partition after flush"
        );
    }

    #[tokio::test]
    async fn search() {
        let schema_ref: SchemaRef = get_schema();

        let no_op_flusher = Arc::new(Mutex::new(NoOpFlusher::new()));

        let mut mem_table =
            CelesMemTable::try_new(Arc::clone(&schema_ref), no_op_flusher).unwrap();

        let (sender, receiver) = channel(100);
        let initiated_stream = stream_data(schema_ref.clone(), sender, 10i64);

        // Create a stream for RecordBatch that will funnel data into the MemTable
        let stream: SendableRecordBatchStream =
            Box::pin(MemTableStream::new(Arc::clone(&schema_ref), receiver));

        // Kick off the stream to begin data transmission
        initiated_stream.send(()).await.unwrap();

        let row_count = mem_table.write_all(stream).await.unwrap();

        assert_eq!(row_count, 10, "Expected to write 10 rows");

        use datafusion::prelude::SessionContext;
        use tokio::time::timeout;

        // create local execution context
        let ctx = SessionContext::new();

        // Register the in-memory table containing the data
        ctx.register_table("users", Arc::new(mem_table)).unwrap();

        let dataframe = ctx.sql("SELECT * FROM users;").await.unwrap();

        timeout(Duration::from_secs(10), async move {
            let result = dataframe.collect().await.unwrap();
            let record_batch = result.get(0).unwrap();

            assert_eq!(1, record_batch.column(0).len());

            dbg!(record_batch.columns());
        })
        .await
        .unwrap();
    }
}
