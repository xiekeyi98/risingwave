// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use crate::executor::ExecutorBuilder;
use crate::executor_v2::{BoxedExecutor, Executor, FilterExecutor};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct FilterExecutorBuilder;

impl ExecutorBuilder for FilterExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::FilterNode)?;
        let search_condition = build_from_prost(node.get_search_condition()?)?;

        Ok(
            FilterExecutor::new(params.input.remove(0), search_condition, params.executor_id)
                .boxed(),
        )
    }
}
