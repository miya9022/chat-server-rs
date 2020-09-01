use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use log::{error, info};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;
use warp::Filter;
use warp::ws::WebSocket;

use crate::client::{RoomClient, UserClient};
use crate::room_storage::RoomStorage;
use crate::hub::HubOptions;
use crate::proto::InputParcel;
use crate::domain::repository::RepositoryFactory;
use crate::user_storage::UserStorage;

pub struct Server {
  port: u16,
  room_storage: Arc<RoomStorage>,
  user_storage: Arc<UserStorage>,
}

impl Server {
  pub fn new(port: u16, repo_fact: RepositoryFactory) -> Self {
    Server {
      port,
      room_storage: Arc::new(RoomStorage::new(
        Some(HubOptions {
          alive_interval: Some(Duration::from_secs(5)),
        }),
        &repo_fact
      )),
      user_storage: Arc::new(UserStorage::new(
        &repo_fact
      )),
    }
  }

  pub async fn run_user(&self, port: u16) {
    let (input_sender, input_receiver) = mpsc::unbounded_channel::<InputParcel>();
    let user_storage = self.user_storage.clone();

    let user = warp::path!("ws"/ String / "rooms")
        .and(warp::ws())
        .and(warp::any().map(move || input_sender.clone()))
        .and(warp::any().map(move || user_storage.clone()))
        .map(
          move |user_id,
                ws: warp::ws::Ws,
                input_sender: UnboundedSender<InputParcel>,
                storage: Arc<UserStorage>| {
            ws.on_upgrade(move |web_socket| async move {
              tokio::spawn(Self::process_user_client(user_id, storage, web_socket, input_sender));
            })
          },
        );

    let shutdown = async {
      tokio::signal::ctrl_c()
          .await
          .expect("failed to install Ctrl+C signal handler");
    };

    let (_, serving) = warp::serve(user).bind_with_graceful_shutdown(([127, 0, 0, 1], port), shutdown);
    let running_storage = self.user_storage.run(input_receiver);

    tokio::select! {
      _ = serving => {},
      _ = running_storage => {},
    }
  }

  pub async fn run_room(&self) {
    let (input_sender, input_receiver) = mpsc::unbounded_channel::<InputParcel>();
    let room_storage = self.room_storage.clone();

    let storage = warp::path!("ws"/ String / "feeds")
      .and(warp::ws())
      .and(warp::any().map(move || input_sender.clone()))
      .and(warp::any().map(move || room_storage.clone()))
      .map(
        move |room_id,
              ws: warp::ws::Ws,
              input_sender: UnboundedSender<InputParcel>,
              storage: Arc<RoomStorage>| {
            ws.on_upgrade(move |web_socket| async move {
              tokio::spawn(Self::process_room_client(room_id, storage, web_socket, input_sender));
            })
          },
      );

    let shutdown = async {
      tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C signal handler");
    };

    let (_, serving) = warp::serve(storage).bind_with_graceful_shutdown(([127, 0, 0, 1], self.port), shutdown);
    let running_storage = self.room_storage.run(input_receiver);

    tokio::select! {
      _ = serving => {},
      _ = running_storage => {},
    }
  }

  async fn process_user_client(
    user_id: String,
    user_storage: Arc<UserStorage>,
    web_socket: WebSocket,
    input_sender: UnboundedSender<InputParcel>,
  ) {
    let output_receiver = user_storage.subscribe();
    let (ws_sink, ws_stream) = web_socket.split();
    let user_client = UserClient::new(user_id);

    let reading = user_client
        .read_input(ws_stream)
        .try_for_each(|input_parcel| async {
          input_sender.send(input_parcel).unwrap();
          Ok(())
        });

    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(rx.forward(ws_sink));

    let writing = user_client
        .write_output(output_receiver.into_stream())
        .try_for_each(|message| async {
          tx.send(Ok(message)).unwrap();
          Ok(())
        });

    if let Err(err) = tokio::select! {
      result = reading => result,
      result = writing => result,
    } {
      error!("Client connection error: {}", err);
    }
  }
  
  async fn process_room_client(
    room_id: String,
    room_storage: Arc<RoomStorage>,
    web_socket: WebSocket,
    input_sender: UnboundedSender<InputParcel>,
  ) {
    let output_receiver = room_storage.subscribe();
    let (ws_sink, ws_stream) = web_socket.split();
    let room_client = RoomClient::new(room_id);

    let reading = room_client
      .read_input(ws_stream)
      .try_for_each(|input_parcel| async {
        input_sender.send(input_parcel).unwrap();
        Ok(())
      });

    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(rx.forward(ws_sink));
    
    let writing = room_client
      .write_output(output_receiver.into_stream())
      .try_for_each(|message| async {
        tx.send(Ok(message)).unwrap();
        Ok(())
      });

    if let Err(err) = tokio::select! {
      result = reading => result,
      result = writing => result,
    } {
      error!("Client connection error: {}", err);
    }
  }
}