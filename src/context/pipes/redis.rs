use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use redis::{Commands, ToRedisArgs};

use crate::context::Level;
use crate::generic_map::AddMap;
use crate::{
    context::{CommitId, Dropped},
    op::Op,
    relation::RelationInner,
    ValueCount,
};

use super::{ProcessResult, Processable};

impl<T: Debug, C> Drop for RedisPipe<T, C> {
    fn drop(&mut self) {
        let mut connection = self.client.get_connection().unwrap();
        for (&(ref value, level), _) in self.values.iter() {
            connection
                .del(RedisKey(&self.name, value, level))
                .unwrap_or_else(|err| log::error!("Redis error: {}", err));
        }
    }
}

pub struct RedisPipe<T: Debug, C> {
    name: String,
    relation: RelationInner<T, C>,
    values: HashMap<(T, Level), ValueCount>,
    client: redis::Client,
    changed_values_scratch: HashMap<(T, Level), ValueCount>,
    changed_keys_scratch: HashSet<(T, Level)>,
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

impl<T: Clone + Eq + Hash + Debug, C: Op<T>> Processable for RedisPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        self.relation
            .dump_to_map(commit_id, &mut self.changed_values_scratch);
        for ((k, level), v) in self.changed_values_scratch.drain() {
            self.changed_keys_scratch.insert((k.clone(), level));
            self.values.add(((k, level), v));
        }
        let mut connection = self.client.get_connection().unwrap();
        for k in self.changed_keys_scratch.drain() {
            match self.values.get(&k) {
                Some(&v) => connection.set(RedisKey(&self.name, &k.0, k.1), v).unwrap(),
                None => connection.del(RedisKey(&self.name, &k.0, k.1)).unwrap(),
            }
        }
        Ok(ProcessResult::Unchanged)
    }
}

struct RedisKey<'a, T>(&'a str, &'a T, Level);

impl<T: Debug> ToRedisArgs for RedisKey<'_, T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let RedisKey(name, key, level) = self;
        out.write_arg(format!("{}:{:?}:{}", name, key, level).as_bytes());
    }
}
