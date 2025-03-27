// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines the progressive eval plan

use std::any::Any;
use std::borrow::Cow::Borrowed;
use std::sync::Arc;

use crate::common::spawn_buffered;
use crate::execution_plan::{Boundedness, EmissionType};
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue,
    MetricsSet,
};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Metric,
    PlanProperties,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue, Statistics};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{Distribution, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use futures::{ready, Stream, StreamExt};
use log::{debug, trace, warn};
use std::pin::Pin;
use std::task::{Context, Poll};

/// ProgressiveEval return a stream of record batches in the order of its inputs.
/// It will stop when the number of output rows reach the given limit.
///
/// This takes an input execution plan and a number n, and provided each partition of
/// the input plan is in an expected order, this operator will return top record batches that covers the top n rows
/// in the order of the input plan.
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │       │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│  ProgressiveEval  │───▶│ │ A │ B ║ C ║ D │ M ║ N ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ M ║ N ║               │──┘                             │
/// │ ╚═══╩═══╝               │                Output only include top record batches that cover top N rows
/// └─────────────────────────┘
///   Stream 2
///
///
///  Input Streams                                             Output stream
///  (in some order)                                           (in same order)
/// ```
#[derive(Debug, Clone)]
pub(crate) struct ProgressiveEvalExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,

    /// Corresponding value ranges of the input plan
    /// None if the value ranges are not available
    value_ranges: Option<Vec<(ScalarValue, ScalarValue)>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,

    /// Grouping of partitions, such that partitions in a group will be executed sequentially.
    partition_groups: Vec<Vec<usize>>,
}

impl ProgressiveEvalExec {
    /// Create a new progressive execution plan
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        value_ranges: Option<Vec<(ScalarValue, ScalarValue)>>,
        fetch: Option<usize>,
        partition_groups: Vec<Vec<usize>>,
    ) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            value_ranges,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch,
            cache,
            partition_groups,
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // progressive eval does not change the equivalence properties of its input
        let eq_properties = input.equivalence_properties().clone();

        // This node serializes all the data to a single partition
        let output_partitioning = Partitioning::UnknownPartitioning(1);

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for ProgressiveEvalExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProgressiveEvalExec: ")?;
                if let Some(fetch) = self.fetch {
                    write!(f, "fetch={fetch}, ")?;
                };
                if let Some(value_ranges) = &self.value_ranges {
                    write!(f, "input_ranges={value_ranges:?}, ")?;
                };
                write!(f, "partition_groups={:?}", self.partition_groups)?;

                Ok(())
            }
            _ => todo!("tree mode"),
        }
    }
}

impl ExecutionPlan for ProgressiveEvalExec {
    fn name(&self) -> &str {
        "ProgressiveEvalExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        let input_ordering = self
            .input()
            .properties()
            .output_ordering()
            .map(|lex_ordering| LexRequirement::from_lex_ordering(lex_ordering.clone()));

        vec![input_ordering]
    }

    /// ProgressiveEvalExec will only accept sorted input
    /// and will maintain the input order
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&children[0]),
            self.value_ranges.clone(),
            self.fetch,
            self.partition_groups.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start ProgressiveEvalExec::execute for partition: {}",
            partition
        );
        if 0 != partition {
            return internal_err!("ProgressiveEvalExec invalid partition {partition}");
        }

        let input_partitions = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        trace!(
            "Number of input partitions of  ProgressiveEvalExec::execute: {}",
            input_partitions
        );
        let schema = self.schema();

        // Add a metric to record the number of inputs
        let num_inputs = Count::new();
        num_inputs.add(
            self.input
                .properties()
                .output_partitioning()
                .partition_count(),
        );
        self.metrics.register(Arc::new(Metric::new(
            MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_inputs,
            },
            None,
        )));
        // Add a metric to record the number of inputs that are actually read which is <= num_inputs
        let num_read_inputs_counter =
            MetricBuilder::new(&self.metrics).global_counter("num_read_inputs");
        // Add other base line metrics
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let result = ProgressiveEvalStream::new(
            Arc::clone(&self.input),
            Arc::clone(&context),
            schema,
            baseline_metrics,
            num_read_inputs_counter,
            self.fetch,
            self.partition_groups.clone(),
        )?;

        debug!("Got stream result from ProgressiveEvalStream::new_from_receivers");

        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        self.input.statistics()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            input: Arc::clone(&self.input),
            value_ranges: self.value_ranges.clone(),
            metrics: self.metrics.clone(),
            fetch: limit,
            cache: self.cache.clone(),
            partition_groups: self.partition_groups.clone(),
        }))
    }
}

/// Handle when to prefetch input streams and how to poll next record batch
struct InputStreams {
    /// Total input streams
    input_stream_count: usize,

    /// Number of input streams to prefetch
    num_input_streams_to_prefetch: usize,

    /// Index of current stream
    current_stream_idx: usize,

    /// Input stream to poll data
    current_input_stream: Option<SendableRecordBatchStream>,

    /// Prefetched Input streams
    prefetched_input_streams: Vec<SendableRecordBatchStream>,

    /// Used to record number of actually read input streams
    num_read_inputs_counter: Count,

    ///
    streams: Vec<SendableRecordBatchStream>,
}

impl InputStreams {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        num_input_streams_to_prefetch: usize,
        num_read_inputs_counter: Count,
        partition_groups: Vec<Vec<usize>>,
    ) -> Result<Self> {
        let input_stream_count = partition_groups.len();

        let current_stream_idx = 0;
        let mut current_input_stream = None;
        let mut capacity = 0;
        if num_input_streams_to_prefetch > 1 {
            capacity = num_input_streams_to_prefetch - 1;
        } else {
            warn!("num_input_streams_to_prefetch is {num_input_streams_to_prefetch} and not greater than 1");
        }
        let mut prefetched_input_streams = Vec::with_capacity(capacity);

        // Concatenate each chain into a single stream.
        let mut streams = partition_groups
            .into_iter()
            .map(|chain| {
                let mut streams = chain
                    .into_iter()
                    .map(|i| input_plan.execute(i, Arc::clone(&context)))
                    .collect::<Result<Vec<_>>>()?;

                // If there's only 1 input partition in this group,
                // no need to concatenate anything.
                if streams.len() == 1 {
                    return Ok(streams.remove(0));
                }

                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    input_plan.schema(),
                    futures::stream::iter(streams).flatten(),
                )) as SendableRecordBatchStream)
            })
            .collect::<Result<Vec<_>>>()?;

        for i in 0..num_input_streams_to_prefetch {
            if i >= input_stream_count {
                break;
            }

            let input_stream = spawn_buffered(streams.remove(0), 1);
            num_read_inputs_counter.add(1);

            if i == 0 {
                current_input_stream = Some(input_stream);
            } else {
                prefetched_input_streams.push(input_stream);
            }
        }

        Ok(Self {
            input_stream_count,
            num_input_streams_to_prefetch,
            current_stream_idx,
            current_input_stream,
            prefetched_input_streams,
            num_read_inputs_counter,
            streams,
        })
    }

    /// Set next available stream to current_input_stream
    /// Also prefetch one more input stream if not all of them are prefetched yet
    fn next_stream(&mut self) {
        // No more input stream
        if self.current_stream_idx >= self.input_stream_count {
            // panic if we have not reached the end of all input streams
            assert!(
                self.prefetched_input_streams.is_empty(),
                "Internal error in ProgressiveEvalStream: There should not have input streams left to read",);

            self.current_input_stream = None;
        } else {
            // prefetch one more input stream before setting next strem to the current input stream
            if self.current_stream_idx + self.num_input_streams_to_prefetch
                < self.input_stream_count
            {
                self.num_read_inputs_counter.add(1);
                self.prefetched_input_streams
                    .push(spawn_buffered(self.streams.remove(0), 1));
            }

            self.current_stream_idx += 1;
            if self.prefetched_input_streams.is_empty() {
                self.current_input_stream = None;
            } else {
                self.current_input_stream = Some(self.prefetched_input_streams.remove(0));
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        // All input streams have been read
        if self.current_input_stream.is_none() {
            return Poll::Ready(None);
        }

        // Get next record batch
        let mut poll;
        loop {
            poll = self
                .current_input_stream
                .as_mut()
                .unwrap()
                .poll_next_unpin(cx);
            match poll {
                // This input stream no longer has data, move to next stream
                Poll::Ready(None) => {
                    self.next_stream();
                    if self.current_input_stream.is_none() {
                        // Have reached the end of all input streams
                        return Poll::Ready(None);
                    }
                }
                _ => break,
            }
        }

        poll
    }
}

/// Concat input streams until reaching the fetch limit
struct ProgressiveEvalStream {
    /// Input streams
    input_streams: InputStreams,

    /// The schema of the input and output.
    schema: SchemaRef,

    /// used to record execution baseline metrics
    baseline_metrics: BaselineMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,
}

impl ProgressiveEvalStream {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
        num_read_inputs_counter: Count,
        fetch: Option<usize>,
        partition_groups: Vec<Vec<usize>>,
    ) -> Result<Self> {
        // Use config param to set number of prefetch stream
        let mut num_input_streams_to_prefetch = context
            .session_config()
            .options()
            .optimizer
            .progressive_eval_num_prefetch_input_streams;

        // If there is no limit of number of rows to fecth, prefetch all input streams
        if fetch.is_none() {
            num_input_streams_to_prefetch = input_plan
                .properties()
                .output_partitioning()
                .partition_count()
        }

        let input_streams = InputStreams::new(
            input_plan,
            context,
            num_input_streams_to_prefetch,
            num_read_inputs_counter,
            partition_groups,
        )?;

        Ok(Self {
            input_streams,
            schema,
            baseline_metrics,
            aborted: false,
            fetch,
            produced: 0,
        })
    }
}

impl Stream for ProgressiveEvalStream {
    type Item = Result<RecordBatch>;

    // Return the next record batch until reaching the fetch limit or the end of all input streams
    // Return pending if the next record batch is not ready
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Error in previous poll
        if self.aborted {
            return Poll::Ready(None);
        }

        // Have reached the fetch limit
        if self.produced >= self.fetch.unwrap_or(std::usize::MAX) {
            return Poll::Ready(None);
        }

        let poll = self.input_streams.poll_next(cx);

        let poll = match ready!(poll) {
            // This input stream has data, return its next record batch
            Some(Ok(batch)) => {
                self.produced += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            // This input stream has an error, return the error and set aborted to true to stop polling next round
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            // This input stream has no more data, return None (aka finished)
            None => {
                // Reaching here means data of all streams have read
                Poll::Ready(None)
            }
        };

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for ProgressiveEvalStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
