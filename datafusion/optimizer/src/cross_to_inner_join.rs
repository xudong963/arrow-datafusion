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

use datafusion_expr::LogicalPlan;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;

#[derive(Default)]
pub struct CrossToInnerJoin;

impl CrossToInnerJoin {
    pub fn new() -> Self {
        Self::default()
    }

}

impl OptimizerRule for CrossToInnerJoin {
    fn optimize(&self, plan: &LogicalPlan, optimizer_config: &OptimizerConfig) -> Result<LogicalPlan> {
        unimplemented!()
    }

    fn name(&self) -> &str {
        "cross_to_inner_join"
    }
}