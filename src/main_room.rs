use chat_server::server::RoomServer;
use chat_server::cass::server_node::ServerNode;
use chat_server::domain::repository::{RepositoryFactory, RepoKind};

#[tokio::main]
async fn main() {
  env_logger::init();
  let _ = init_cassandra_cluster().await.unwrap();
  let mut repo_factory = RepositoryFactory::new();

  repo_factory.add_repository(ServerNode::build_cass_cluster().unwrap(), RepoKind::ROOM);
  repo_factory.add_repository(ServerNode::build_cass_cluster().unwrap(), RepoKind::USER);
  repo_factory.add_repository(ServerNode::build_cass_cluster().unwrap(), RepoKind::MESSAGE);
  repo_factory.add_repository(ServerNode::build_cass_cluster().unwrap(), RepoKind::ROOM_USERS);

  let server = RoomServer::new(8889, repo_factory);
  server.run().await;
}

async fn init_cassandra_cluster() -> Option<ServerNode> {
  let mut node = ServerNode::new();
  match node.init().await {
    Ok(_) => {
      println!("create cluster success");
      Some(node)
    },
    _ => {
      println!("error occur");
      None
    }
  }
}