use cassandra_cpp::*;
use crate::cass::schema_loader::SchemaLoader;

#[derive(Default)]
pub struct ServerNode {
    cluster_instance: Cluster,
}

impl ServerNode {

    pub fn new() -> Self {
        ServerNode {
            cluster_instance: Cluster::default()
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.cluster_instance.set_contact_points("127.0.0.1").unwrap();
        self.cluster_instance.set_load_balance_round_robin();

        match self.cluster_instance.connect() {
            Ok(ref mut session) => {
                let schema_loader = SchemaLoader::new_with_session(session);
                ServerNode::load_schema_from_file(&schema_loader, "/keyspace.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "/room.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "/user.cql").await;
                ServerNode::load_schema_from_file(&schema_loader, "/message.cql").await;
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
            _ => {
                println!("Error occur");
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