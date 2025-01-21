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

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::{
    coalesce_batches::CoalesceBatches, enforce_distribution::EnforceDistribution,
    output_requirements::OutputRequirements, PhysicalOptimizerRule,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::*;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::test]
async fn apply_enforce_distribution_multiple_times() -> Result<()> {
    // Create a configuration
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);

    // Create table schema and data
    // To reproduce to bug: the LOCATION should contain more than one aggregate_test_100.csv
    let sql = "CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  TINYINT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT,
        c5  INT,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  BIGINT UNSIGNED NOT NULL,
        c10 VARCHAR NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    LOCATION '../../testing/data/csv/aggregate_test_100.csv'
    OPTIONS ('format.has_header' 'true')";

    ctx.sql(sql).await?;

    let df = ctx.sql("SELECT * FROM(SELECT * FROM aggregate_test_100 UNION ALL SELECT * FROM aggregate_test_100) ORDER BY c13 LIMIT 5").await?;
    let logical_plan = df.logical_plan().clone();
    let analyzed_logical_plan = ctx.state().analyzer().execute_and_check(
        logical_plan,
        ctx.state().config_options(),
        |_, _| (),
    )?;

    let optimized_logical_plan = ctx.state().optimizer().optimize(
        analyzed_logical_plan,
        &ctx.state(),
        |_, _| (),
    )?;

    let optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
        Arc::new(OutputRequirements::new_add_mode()),
        Arc::new(EnforceDistribution::new()),
        Arc::new(EnforceSorting::new()),
        Arc::new(ProjectionPushdown::new()),
        Arc::new(CoalesceBatches::new()),
        Arc::new(EnforceDistribution::new()), // -- Add enforce distribution rule again
        Arc::new(OutputRequirements::new_remove_mode()),
        Arc::new(ProjectionPushdown::new()),
        Arc::new(LimitPushdown::new()),
        Arc::new(SanityCheckPlan::new()),
    ];

    let planner = DefaultPhysicalPlanner::default();
    let session_state = SessionStateBuilder::new()
        .with_config(ctx.copied_config())
        .with_default_features()
        .with_physical_optimizer_rules(optimizers)
        .build();
    let optimized_physical_plan = planner
        .create_physical_plan(&optimized_logical_plan, &session_state)
        .await?;

    let mut results = optimized_physical_plan
        .execute(0, ctx.task_ctx().clone())
        .unwrap();

    let batch = results.next().await.unwrap()?;
    // With the fix of https://github.com/apache/datafusion/pull/14207, the number of rows will be 10
    assert_eq!(batch.num_rows(), 5);
    Ok(())
}
