use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use async_trait::async_trait;
use cassandra_cpp::*;
use chrono::{NaiveDateTime, DateTime, Utc};
use uuid::Uuid;

use crate::model::user::User;
use tokio::sync::Mutex;
use crate::domain::room_repository::RoomRepository;
use crate::domain::room_user_repository::RoomUserRepository;
use crate::domain::user_repository::UserRepository;
use crate::domain::message_repository::MessageRepository;

#[derive(Default)]
pub struct RepositoryFactory(HashMap<String, Arc<dyn Any>>);

impl RepositoryFactory {

    pub fn new() -> Self {
        RepositoryFactory(HashMap::<String, Arc<dyn Any>>::new())
    }

    pub fn add_repository(&mut self, cluster: Cluster, kind: RepoKind) {
        let (key, repo): (&str, Arc<dyn Any>) = match kind {
            RepoKind::ROOM => (
                "ROOM",
                Arc::new(RoomRepository {
                    cluster: Mutex::new(cluster)
                })
            ),
            RepoKind::USER => (
                "USER",
                Arc::new(UserRepository {
                    cluster: Mutex::new(cluster)
                })
            ),
            RepoKind::ROOM_USERS => (
                "ROOM_USERS",
                Arc::new(RoomUserRepository {
                    cluster: Mutex::new(cluster)
                })
            ),
            RepoKind::MESSAGE => (
                "MESSAGE", 
                Arc::new(MessageRepository {
                    cluster: Mutex::new(cluster)
                })
            ),
        };
        self.0.insert(key.to_string(), repo);
    }

    pub fn get_repository(&self, repo_name: &str) -> Arc<dyn Any> {
        match self.0.get(repo_name) {
            Some(repo) => Arc::clone(repo),
            None => panic!("Not supported")
        }
    }
}

pub enum RepoKind {
    ROOM,
    USER,
    ROOM_USERS,
    MESSAGE,
}

#[async_trait]
pub trait Repository {

    async fn retrieve_session(&self) -> Result<Session>;
}

pub struct Utils {}

impl Utils {

    const SEPARATOR_CHARS: &'static str = "*&&*";

    pub fn from_uuid_to_cass_uuid(uuid: Uuid) -> cassandra_cpp::Uuid {
        cassandra_cpp::Uuid::from_str( uuid.to_string().as_str() ).ok().unwrap()
    }

    pub fn from_cass_uuid_to_uuid(cass_uuid: cassandra_cpp::Uuid) -> Uuid {
        Uuid::from_str(cass_uuid.to_string().as_str()).ok().unwrap()
    }

    pub fn from_timestamp_to_datetime(timestamp: i64) -> DateTime<Utc> {
        let datetime = NaiveDateTime::from_timestamp(timestamp, 0);
        DateTime::from_utc(datetime, Utc)
    }

    fn get_participants(participants: SetIterator) -> Option<Vec<User>> {
        Some(
            participants.map(|participant| {
                let part_record = Result::ok(participant.get_string()).unwrap();
                let items: Vec<&str> = part_record.split(Self::SEPARATOR_CHARS).collect();
                User {
                    id: Uuid::from_str(items.get(0).unwrap()).unwrap(),
                    name: items.get(1).unwrap().to_string(),
                }
            }).collect()
        )
    }
}