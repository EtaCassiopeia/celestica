// #![feature(async_fn_in_trait)]

use std::any::Any;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::datasource::memory::PartitionData;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::insert::{DataSink, InsertExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, TryStreamExt};
use futures::{Future, FutureExt, StreamExt, TryFutureExt};
use lancedb::error::Error as LanceError;
use lancedb::table::Table;
use log::{debug, info, log};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use crate::errors::LanceErrorWrapper;
use crate::fs::create_if_not_exists;

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
            // max_rows: 10000,
            max_rows: 2,
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

#[async_trait]
pub trait Flushable: Send + Sync {
    async fn flush(
        &mut self,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Clone)]
pub struct TableFlusher {
    base_url: String,
    table: String,
}

impl TableFlusher {
    pub fn new(base_url: String, table: String) -> Self {
        Self { base_url, table }
    }
}

#[async_trait]
impl Flushable for TableFlusher {
    async fn flush(
        &mut self,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create the the base directory to store data if it doesn't exist.
        create_if_not_exists(&self.base_url).await?;

        // How to check if a type is Send
        // rustup install nightly
        // RUSTFLAGS="-Z macro-backtrace" cargo run -p celestica-cli
        // assert_impl_all!(Box<dyn RecordBatchReader<Item=std::result::Result<RecordBatch, arrow_schema::ArrowError>>> : Send);

        let batches: Vec<RecordBatch> = partitions.iter().flatten().cloned().collect();

        let schema = batches[0].schema();
        let buffer = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        // Open the table, or create it if it doesn't exist.
        match Table::open(&self.base_url).await {
            Ok(mut table) => {
                info!("Flushing batches to the existing table");
                println!("Flushing batches to the existing table");
                table.add(buffer, None).await
            },
            Err(LanceError::TableNotFound { .. }) => {
                info!("Creating a new table");
                println!("Creating a new table");
                Table::create(&self.base_url, &self.table, buffer, None)
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
    pub(crate) batches: Vec<PartitionData>,
    flusher: Arc<Mutex<dyn Flushable>>,
    options: MemTableOptions,
}

impl CelesMemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(
        schema: SchemaRef,
        flusher: Arc<Mutex<dyn Flushable>>,
    ) -> Result<Self> {
        let options = MemTableOptions::default();
        Ok(Self {
            schema,
            batches: vec![Arc::new(RwLock::new(vec![])); options.partitions],
            flusher,
            options,
        })
    }

    pub fn try_new_with_options(
        schema: SchemaRef,
        flusher: Arc<Mutex<dyn Flushable>>,
        options: MemTableOptions,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            batches: vec![Arc::new(RwLock::new(vec![])); options.partitions],
            flusher,
            options,
        })
    }

    fn init_empty_partitions(
        num_partitions: usize,
        vector_size: usize,
    ) -> Vec<PartitionData> {
        let mut partitions: Vec<PartitionData> = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            let data = Arc::new(RwLock::new(Vec::with_capacity(vector_size)));
            partitions.push(data);
        }

        partitions
    }

    pub fn with_options(mut self, options: MemTableOptions) -> Self {
        self.options = options;
        self
    }

    async fn write_all(&mut self, mut data: SendableRecordBatchStream) -> Result<u64> {
        let num_partitions = self.batches.len();

        println!("num_partitions: {}", num_partitions);

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

        let should_flush = self.should_flush().await;
        if should_flush {
            self.flush().await?;
        }

        Ok(row_count as u64)
    }

    pub async fn row_count(&self) -> Result<usize> {
        let mut total_rows = 0;

        for partition in &self.batches {
            let record_batches: RwLockReadGuard<Vec<RecordBatch>> =
                partition.read().await;
            for batch in record_batches.iter() {
                total_rows += batch.num_rows();
            }
        }

        Ok(total_rows)
    }

    async fn should_flush(&self) -> bool {
        self.row_count().await.unwrap_or(0) >= self.options.max_rows
    }

    async fn convert_partitions(&self) -> Vec<Vec<RecordBatch>> {
        let mut new_partitions: Vec<Vec<RecordBatch>> = Vec::new();

        for partition in &self.batches {
            let record_batches: RwLockReadGuard<Vec<RecordBatch>> =
                partition.read().await;
            new_partitions.push(record_batches.clone());
        }

        new_partitions
    }

    async fn flush(&mut self) -> Result<()> {
        info!("Flushing memtable");
        let mut flusher = self.flusher.lock().await;
        let partitions: Vec<Vec<RecordBatch>> = self.convert_partitions().await;
        flusher.flush(partitions).await?;

        self.batches.clear();
        Ok(())
    }
}

impl Debug for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTable").finish()
    }
}

impl Display for CelesMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryTable ")
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

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !input.schema().eq(&self.schema) {
            return Err(DataFusionError::Plan(
                "Inserting query must have the same schema with the table.".to_string(),
            ));
        }
        let sink = Arc::new(MemSink::new(
            self.batches.clone(),
            self.flusher.clone(),
            self.options.max_rows,
        ));
        Ok(Arc::new(InsertExec::new(input, sink)))
    }
}

/// Implements for writing to a [`MemTable`]
struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
    flusher: Arc<Mutex<dyn Flushable>>,
    max_rows: usize,
}

impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl Display for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let partition_count = self.batches.len();
        write!(f, "MemoryTable (partitions={partition_count})")
    }
}

impl DisplayAs for MemSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_count = self.batches.len();
                write!(f, "MemoryTable (partitions={partition_count})")
            },
        }
    }
}

impl MemSink {
    fn new(
        batches: Vec<PartitionData>,
        flusher: Arc<Mutex<dyn Flushable>>,
        max_rows: usize,
    ) -> Self {
        Self {
            batches,
            flusher,
            max_rows,
        }
    }

    async fn row_count(&self) -> Result<usize> {
        let mut total_rows = 0;

        for partition in &self.batches {
            let record_batches: RwLockReadGuard<Vec<RecordBatch>> =
                partition.read().await;
            for batch in record_batches.iter() {
                total_rows += batch.num_rows();
            }
        }

        Ok(total_rows)
    }

    async fn should_flush(&self) -> bool {
        let row_count = self.row_count().await.unwrap_or(0);
        println!("Row count {}", row_count);
        row_count >= self.max_rows
    }

    async fn convert_partitions(&self) -> Vec<Vec<RecordBatch>> {
        let mut new_partitions: Vec<Vec<RecordBatch>> = Vec::new();

        for partition in &self.batches {
            let record_batches: RwLockReadGuard<Vec<RecordBatch>> =
                partition.read().await;
            new_partitions.push(record_batches.clone());
        }

        new_partitions
    }
}

#[async_trait]
impl DataSink for MemSink {
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
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

        if self.should_flush().await {
            println!("Flushing memtable {}", row_count);
            let flusher: Arc<Mutex<dyn Flushable>> = self.flusher.clone();
            let future = async move {
                let mut flusher = flusher.lock().await;
                let partitions: Vec<Vec<RecordBatch>> = self.convert_partitions().await;
                flusher.flush(partitions).await
            };

            Ok(future
                .and_then(|_| async move {
                    for target in self.batches.iter() {
                        // clear all the batches
                        println!("Clearing batches");
                        target.write().await.clear();
                    }
                    Ok(())
                })
                .map(|_| row_count as u64)
                .await)
        } else {
            Ok(row_count as u64)
        }
    }
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

    #[async_trait]
    impl Flushable for NoOpFlusher {
        async fn flush(
            &mut self,
            _partitions: Vec<Vec<RecordBatch>>,
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
            mem_table.row_count().await.unwrap_or(0),
            10,
            "Expected to have 10 rows in the MemTable"
        );

        //TODO fix me

        // for i in 0..10 {
        //     assert_eq!(
        //         mem_table.partitions[0].read().await[i]
        //             .column_by_name("id")
        //             .unwrap()
        //             .as_any()
        //             .downcast_ref::<StringArray>()
        //             .unwrap()
        //             .value(0),
        //         format!("id-{}", i)
        //     );
        // }
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
            println!("Trying to open table: {} at {}", table_name, uri);
            match Table::open(uri).await {
                Ok(table) => {
                    let written_rows = table.count_rows().await.unwrap();
                    assert_eq!(written_rows, 15, "Expected to write 15 rows");
                    break;
                },
                Err(err) => {
                    if retries == 1 {
                        panic!("Failed to open table after several attempts: {err}");
                    }
                    retries -= 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
            }
        }

        assert_eq!(
            mem_table.row_count().await.unwrap_or(0),
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
