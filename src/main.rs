use chat_server::server::Server;
use chat_server::cass::server_node::ServerNode;

#[tokio::main]
async fn main() {
  env_logger::init();
  let node = init_cassandra_cluster().await.unwrap();

  let server = Server::new(8080);
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