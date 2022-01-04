use std::collections::HashMap;

use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::{Keyspace, StateStore};

use super::{serialize_cell, serialize_cell_idx, serialize_pk};
use crate::executor::managed_state::aggregation::OrderedRowsSerializer;

/// `ManagedMviewState` buffers recent mutations. Data will be written
/// to backend storage on calling `flush`.
pub struct ManagedMViewState<S: StateStore> {
    keyspace: Keyspace<S>,
    schema: Schema,
    pk_columns: Vec<usize>,
    sort_key_serializer: OrderedRowsSerializer,
    memtable: HashMap<Row, Option<Row>>,
}

impl<S: StateStore> ManagedMViewState<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
    ) -> Self {
        // We use `0..` because `mview_sink` would assemble pk for us.
        // Therefore, we don't need the original pk indices any more.
        let order_pairs = orderings.into_iter().zip(0..).collect::<Vec<_>>();

        Self {
            keyspace,
            schema,
            pk_columns,
            memtable: HashMap::new(),
            sort_key_serializer: OrderedRowsSerializer::new(order_pairs),
        }
    }

    pub fn put(&mut self, pk: Row, value: Row) {
        self.memtable.insert(pk, Some(value));
    }

    pub fn delete(&mut self, pk: Row) {
        self.memtable.insert(pk, None);
    }

    // TODO(MrCroxx): flush can be performed in another coruntine by taking the snapshot of
    // memtable.
    pub async fn flush(&mut self, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        batch.reserve(self.memtable.len() * self.schema.len());
        let mut local = batch.local(&self.keyspace);

        for (pk, cells) in self.memtable.drain() {
            debug_assert_eq!(self.pk_columns.len(), pk.0.len());
            let pk_buf = serialize_pk(&pk, &self.sort_key_serializer)?;
            for cell_idx in 0..self.schema.len() {
                // TODO(MrCroxx): More efficient encoding is needed.
                // format: [ pk_buf | cell_idx (4B)]
                let key = [
                    pk_buf.as_slice(),
                    serialize_cell_idx(cell_idx as u32)?.as_slice(),
                ]
                .concat();

                match &cells {
                    Some(cells) => local.put(key, serialize_cell(&cells[cell_idx])?),
                    None => local.delete(key),
                };
            }
        }

        batch.ingest(epoch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::Int32Type;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_mview_state() {
        // Only assert pk and columns can be successfully put/delete/flush,
        // and the ammount of rows is expected.
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let pk_columns = vec![0];
        let orderings = vec![OrderType::Ascending];
        let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);

        let mut state = ManagedMViewState::new(keyspace.clone(), schema, pk_columns, orderings);
        let mut epoch: u64 = 0;
        state.put(
            Row(vec![Some(1_i32.into())]),
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        );
        state.put(
            Row(vec![Some(2_i32.into())]),
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        );
        state.put(
            Row(vec![Some(3_i32.into())]),
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
        );
        state.delete(Row(vec![Some(2_i32.into())]));

        state.flush(epoch).await.unwrap();
        let data = keyspace.scan(None).await.unwrap();
        // cell-based storage has 4 cells
        assert_eq!(data.len(), 4);

        epoch += 1;
        state.delete(Row(vec![Some(3_i32.into())]));
        state.flush(epoch).await.unwrap();
        let data = keyspace.scan(None).await.unwrap();
        assert_eq!(data.len(), 2);
    }
}