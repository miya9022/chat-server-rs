use cassandra_cpp::*;
use std::cell::{RefCell, Ref};
use std::borrow::{BorrowMut, Borrow};
use tokio::sync::{Mutex, MutexGuard};

use crate::cass::schema_loader::SchemaLoader;

#[derive(Default)]
pub struct ServerNode {
    cluster_instance: Cluster,
}

impl ServerNode {

    pub fn new() -> Self {
        ServerNode {
            cluster_instance: Cluster::default(),
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        match self.init_session().await {
            Ok(ref mut session) => {
                let schema_loader = SchemaLoader::new_with_session(session);
                Self::load_schema_from_file(&schema_loader, "cql/keyspace.cql").await;
                Self::load_schema_from_file(&schema_loader, "cql/room.cql").await;
                Self::load_schema_from_file(&schema_loader, "cql/user.cql").await;
                Self::load_schema_from_file(&schema_loader, "cql/room_users.cql").await;
                Self::load_schema_from_file(&schema_loader, "cql/message.cql").await;

                Ok(())
            },
            _ => panic!()
        }
    }
    
    async fn init_session(&mut self) -> Result<Session> {
        self.cluster_instance.set_contact_points("127.0.0.1").unwrap();
        self.cluster_instance.set_load_balance_round_robin();
        
        if let Err(error) = self.cluster_instance.set_protocol_version(4) {
            println!("{:?}", error);
        }
        
        self.cluster_instance.connect_async().await
    }

    pub fn build_cass_cluster() -> Result<Cluster> {
        let mut cluster_instance = Cluster::default();
        cluster_instance.set_contact_points("127.0.0.1").unwrap();
        cluster_instance.set_load_balance_round_robin();

        if let Err(error) = cluster_instance.set_protocol_version(4) {
            println!("{:?}", error);
            Err(error)
        }
        else {
            Ok(cluster_instance)
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