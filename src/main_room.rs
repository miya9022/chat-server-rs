use chat_server::server::Server;
use chat_server::cass::server_node::ServerNode;
use chat_server::domain::repository::{RepositoryFactory, RepoKind};

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut node = init_cassandra_cluster().await.unwrap();
    let mut repo_factory = RepositoryFactory::new();
    match node.init_session().await {
        Ok(ref mut session) => {
            repo_factory.add_repository(session, RepoKind::ROOM);
            repo_factory.add_repository(session, RepoKind::USER);
            repo_factory.add_repository(session, RepoKind::ROOM_USERS);
            repo_factory.add_repository(session, RepoKind::MESSAGE);
        },
        Err(error) => {
            println!("{:?}", error);
        }
    };

    let server = Server::new(8889, repo_factory);
    server.run_user(8889).await;
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