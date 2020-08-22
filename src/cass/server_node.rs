use cassandra_cpp::*;
use crate::cass::schema_loader::SchemaLoader;
use crate::domain::repository::{RepositoryFactory, RepoKind};
use std::sync::Arc;
use std::cell::{RefCell, Ref};
use std::borrow::{BorrowMut, Borrow};
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct ServerNode {
    cluster_instance: Cluster,
    repo_factory: Arc<Mutex<RepositoryFactory>>,
}

impl ServerNode {

    pub fn new() -> Self {
        ServerNode {
            cluster_instance: Cluster::default(),
            repo_factory: Arc::new(
                Mutex::new(
                    RepositoryFactory::new()
                )
            ),
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.cluster_instance.set_contact_points("127.0.0.1").unwrap();
        self.cluster_instance.set_load_balance_round_robin();

        if let Err(error) = self.cluster_instance.set_protocol_version(4) {
            println!("{:?}", error);
        }

        match self.cluster_instance.connect_async().await {
            Ok(ref mut session) => {
                let schema_loader = SchemaLoader::new_with_session(session);
                ServerNode::load_schema_from_file(&schema_loader, "cql/keyspace.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "cql/room.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "cql/user.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "cql/message.cql").await;

                self.repo_factory.lock().await.add_repository(session, RepoKind::ROOM);
                self.repo_factory.lock().await.add_repository(session, RepoKind::USER);
                self.repo_factory.lock().await.add_repository(session, RepoKind::MESSAGE);

                Ok(())
            },
            _ => panic!()
        }
    }

    async fn load_schema_from_file(schema_loader: &SchemaLoader, file_path: &str) {
        match schema_loader.load_from_file(file_path.to_string()).await {
            Ok(result) => {
                println!("{}", result.to_string());
            },
            Err(error) => {
                panic!("Error occur: {:?}", error);
            }
        }
    }

    pub async fn get_repo_factory(&self) -> MutexGuard<'_, RepositoryFactory> {
        self.repo_factory.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn test001_create_cluster_instance_and_load_schema() {
        let mut node = ServerNode::new();
        match aw!(node.init()) {
            Ok(_) => {
                println!("create cluster success")
            },
            _ => {
                println!("error occur")
            }
        }
    }
}