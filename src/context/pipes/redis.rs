use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use redis::{Commands, ToRedisArgs};

use crate::generic_map::AddMap;
use crate::{
    context::{CommitId, Dropped},
    op::Op,
    relation::RelationInner,
    ValueCount,
};

use super::{PipeT, ProcessResult};

impl<T: Debug, C> Drop for RedisPipe<T, C> {
    fn drop(&mut self) {
        for (value, _) in self.values.iter() {
            let mut connection = self.client.get_connection().unwrap();
            connection
                .del(RedisKey(&self.name, value))
                .unwrap_or_else(|err| log::error!("Redis error: {}", err));
        }
    }
}

pub struct RedisPipe<T: Debug, C> {
    name: String,
    relation: RelationInner<T, C>,
    values: HashMap<T, ValueCount>,
    client: redis::Client,
    changed_values_scratch: HashMap<T, ValueCount>,
    changed_keys_scratch: HashSet<T>,
}
impl<T: Debug, C> RedisPipe<T, C> {
    pub(crate) fn new(name: String, relation: RelationInner<T, C>, client: redis::Client) -> Self {
        Self {
            name,
            relation,
            values: HashMap::new(),
            client,
            changed_values_scratch: HashMap::new(),
            changed_keys_scratch: HashSet::new(),
        }
    }
}

impl<T: Clone + Eq + Hash + Debug, C: Op<T>> PipeT for RedisPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        self.relation
            .dump_to_map(commit_id, &mut self.changed_values_scratch);
        for (k, v) in self.changed_values_scratch.drain() {
            self.changed_keys_scratch.insert(k.clone());
            self.values.add((k, v));
        }
        let mut connection = self.client.get_connection().unwrap();
        for k in self.changed_keys_scratch.drain() {
            match self.values.get(&k) {
                Some(&v) => connection.set(RedisKey(&self.name, &k), v).unwrap(),
                None => connection.del(RedisKey(&self.name, &k)).unwrap(),
            }
        }
        Ok(ProcessResult::Unchanged)
    }
    fn push_frame(&mut self) {
        ()
    }
    fn pop_frame(&mut self, _commit_id: CommitId) -> Result<(), Dropped> {
        Ok(())
    }
}

struct RedisKey<'a, T>(&'a str, &'a T);

impl<T: Debug> ToRedisArgs for RedisKey<'_, T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let RedisKey(name, key) = self;
        out.write_arg(format!("{}:{:?}", name, key).as_bytes());
    }
}
