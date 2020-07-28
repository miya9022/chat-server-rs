use chat_server::server::Server;

#[tokio::main]
async fn main() {
  env_logger::init();

  let server = Server::new(8080);
  server.run().await;
}
