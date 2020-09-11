use std::collections::HashMap;
use std::sync::Arc;
use futures::StreamExt;
use uuid::Uuid;
use chrono::Utc;
use tokio::sync::{ RwLock, broadcast, mpsc::UnboundedReceiver };
use tokio::time;

use crate::model::{room::Room, user::User};
use crate::hub::{Hub, HubOptions};
use crate::proto::*;
use crate::utils::AppUtils;
use crate::model::room_user::RoomUser;
use crate::domain::room_repository::RoomRepository;
use crate::domain::user_repository::UserRepository;
use crate::domain::message_repository::MessageRepository;
use crate::domain::room_user_repository::RoomUserRepository;
use crate::domain::repository::RepositoryFactory;

const OUTPUT_CHANNEL_SIZE: usize = 256;

pub struct RoomStorage {
  output_sender: broadcast::Sender<OutputParcel>,
  rooms: RwLock<HashMap<String, Arc<Room>>>,
  hubs: RwLock<HashMap<String, Arc<Hub>>>,
  hub_options: Option<HubOptions>,

  room_repository: Arc<RoomRepository>,
  user_repository: Arc<UserRepository>,
  message_repository: Arc<MessageRepository>,
  room_user_repository: Arc<RoomUserRepository>,
}

impl RoomStorage {
  pub fn new(hub_options: Option<HubOptions>, repo_fact: &RepositoryFactory) -> Self {
    let (output_sender, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);

    let room_repository = match AppUtils::downcast_arc::<RoomRepository>(
      repo_fact.get_repository("ROOM")) {
      Ok(repo) => repo,
      Err(_) => panic!("can't find repository")
    };

    let user_repository = match AppUtils::downcast_arc::<UserRepository>(
      repo_fact.get_repository("USER")) {
      Ok(repo) => repo,
      Err(_) => panic!("can't find repository")
    };

    let message_repository = match AppUtils::downcast_arc::<MessageRepository>(
      repo_fact.get_repository("MESSAGE")) {
      Ok(repo) => repo,
      Err(_) => panic!("can't find repository")
    };

    let room_user_repository = match AppUtils::downcast_arc::<RoomUserRepository>(
      repo_fact.get_repository("ROOM_USERS")) {
      Ok(repo) => repo,
      Err(_) => panic!("can't find repository")
    };

    RoomStorage {
      output_sender,
      rooms: Default::default(),
      hubs: Default::default(),
      hub_options,
      room_repository,
      user_repository,
      message_repository,
      room_user_repository,
    }
  }

  async fn get_room(&self, room_id: &str) -> Option<Arc<Room>> {
    let map = self.rooms.read().await;
    if let Some(room) = map.get(room_id) {
      Some(Arc::clone(room))
    }
    else {
      match self.room_repository.load_one_room(room_id).await {
        None => None,
        Some(room) => {
          self.insert_room(room_id, room.clone()).await
        }
      }
    }
  }

  async fn insert_room(&self, room_id: &str, room: Room) -> Option<Arc<Room>> {
    let mut rooms = self.rooms.write().await;
    rooms.insert(room_id.to_string(), Arc::new(room))
  }

  async fn get_hub(&self, room_id: &str) -> Option<Arc<Hub>> {
    let map = self.hubs.read().await;
    if let Some(hub) = map.get(room_id) {
      Some(Arc::clone(hub))
    } else {
      None
    }
  }

  pub fn subscribe(&self) -> broadcast::Receiver<OutputParcel> {
    self.output_sender.subscribe()
  }

  async fn tick_alive(&self) {
    let alive_interval = if let Some(alive_interval) = self.hub_options.unwrap().alive_interval {
      alive_interval
    } else {
      return;
    };

    loop {
      time::delay_for(alive_interval).await;
      self.rooms.read().await.keys().for_each(|room_id| {
        self.output_sender
          .send(OutputParcel::new(
            String::from(room_id), Default::default(), Output::Alive))
          .unwrap();
      })
    }
  }

  pub async fn run(&self, receiver: UnboundedReceiver<InputParcel>) {
    let ticking_alive = self.tick_alive();
    let processing = receiver.for_each(|input_parcel| self.process(input_parcel));

    tokio::select! {
      _ = ticking_alive => {},
      _ = processing => {},
    }
  }

  async fn process(&self, input_parcel: InputParcel) {
    match input_parcel.input {
      Input::Ping => self.send_pong(input_parcel),
      Input::LoadRoom => self.load_room(input_parcel.room_id).await,
      Input::CreateRoom(room_input) => self.create_room(input_parcel.room_id, room_input).await,
      Input::DeleteRoom(remove_room_input) => self.delete_room(remove_room_input).await,
      _ => match self.get_hub(input_parcel.room_id.as_str()).await {
        Some(hub) => {
          hub.process(input_parcel).await;
        },
        None => self.send_error(input_parcel.room_id.as_str(), OutputError::RoomNotExists)
      }
    }
  }

  fn send_pong(&self, input_parcel: InputParcel) {
    self.output_sender
        .send(OutputParcel::new(input_parcel.room_id, input_parcel.client_id, Output::Pong))
        .unwrap();
  }

  async fn load_room(&self, room_id: String) {
    println!("{}", room_id);

    // check room exists
    if !self.rooms.read().await.contains_key(room_id.as_str()) &&
        !self.room_repository.room_exists(room_id.as_str()).await {
      self.send_error(room_id.as_str(), OutputError::RoomNotExists);
      return;
    }

    // get room instance
    if let Some(room) = self.get_room(room_id.as_str()).await {

      // get hub instance
      let hub: Option<Arc<Hub>> = match self.get_hub(room.room_id.as_str()).await {
        Some(hub) => Some(hub),

        // if hub hasn't exists yet, create a new one with room respectively
        None => {
          if let Some(hub) = self.hubs.write().await.insert(room_id.clone(), self.new_hub()) {
            Some(hub)
          }
          else {
            None
          }
        }
      };

      // hub notify all load user and load most recent messages
      if let Some(hub) = hub {
        hub.process(
          InputParcel::new(Uuid::default(), room_id, Input::LoadRoom)
        ).await
      }
    }
  }

  async fn create_room(&self, room_id: String, input: RoomInput) {

    // check room_id exits
    if self.rooms.read().await.contains_key(room_id.as_str()) {
      self.send_error(room_id.as_str(), OutputError::RoomNameTaken);
      return;
    }

    // TODO: check room_id valid

    // setup and serve Room instance 
    let users = input.participants.as_ref()
      .filter(|parts| !parts.is_empty())
      .map(|parts| {
        (*parts).iter()
          .map(|part| User::new(part.id, part.name.as_str()))
          .collect()
      });

    let room = Room::new(room_id.clone(), input.room_title, input.host_id, input.host_name.clone(),
                         users.clone(), Utc::now(), input.delete_key);
    self.rooms.write().await.insert(room_id.clone(), Arc::new(room.clone()));

    // create Hub
    let hub = Hub::new(self.hub_options.unwrap(), self.output_sender.clone(),
                       Arc::clone(&self.user_repository), Arc::clone(&self.message_repository));

    // invite host
    let host_input = InputParcel::new(input.host_id, room_id.clone(),
                                      Input::JoinRoom( JoinInput{ client_id: input.host_id, name: input.host_name } ));
    hub.process(host_input).await;

    // invite participants
    let inputs = users.as_ref()
      .filter(|us| !us.is_empty())
      .map(|us| {
        (*us).iter()
          .map(|user| {
            InputParcel::new(user.id, room_id.clone(),
                             Input::JoinRoom( JoinInput{ client_id: user.id, name: user.name.clone() } ))
          })
          .collect::<Vec<InputParcel>>()
      });

    if let Some(parcels) = inputs.as_ref().filter(|ins| !ins.is_empty()).take() {
      for input in (*parcels).to_owned() {
        hub.process(input).await;
      }
    }

    // serve hub instance
    self.hubs.write().await.insert(room_id.clone(), Arc::new(hub));

    // serve room to database
    if let Some(room) = self.room_repository.create_room(room).await {
      let mut users = vec![];
      users.push(room.host_info);
      if let Some(mut parts) = room.participants {
        users.append(&mut parts);
      }

      for user in users {
        let room_user = RoomUser::new(
          room.room_id.clone(),
          room.room_title.clone(),
          user.id,
          room.create_at,
        );
        self.room_user_repository.create_room_users(room_user).await;
      }
    }

    // send created notification
    self.output_sender
      .send(OutputParcel::new(room_id.clone(), Default::default(),
                              Output::RoomCreated(RoomCreatedOutput::new(room_id))))
      .unwrap();
  }

  async fn delete_room(&self, remove_room_input: RemoveRoomInput) {
    let room_id = remove_room_input.room_id;
    let delete_key = remove_room_input.delete_key;

    if let Some(room) = self.get_room(room_id.as_str()).await {
      if room.delete_key != delete_key {
        self.send_error(room_id.as_str(), OutputError::RemoveRoomFailed);
      }
    } else {
      self.send_error(room_id.as_str(), OutputError::RoomNotExists);
      return;
    }

    // delete room instance
    self.rooms.write().await.remove(room_id.as_str()).unwrap();
    let hub = self.hubs.write().await.remove(room_id.as_str()).unwrap();
    drop(hub);

    // send removed notification
    self.output_sender
      .send(OutputParcel::new(room_id.clone(), Default::default(),
                              Output::RoomRemoved(
                                RoomRemovedOutput::new(room_id.clone()))))
      .unwrap();

    // delete room from db
    self.room_repository.delete_room(room_id.as_str()).await.ok();
  }

  fn new_hub(&self) -> Arc<Hub> {
    Arc::new(
      Hub::new(self.hub_options.unwrap(), self.output_sender.clone(),
             Arc::clone(&self.user_repository), Arc::clone(&self.message_repository))
    )
  }

  fn send_error(&self, room_id: &str, error: OutputError) {
    self.output_sender
      .send(OutputParcel::new(String::from(room_id),
                              Default::default(), Output::Error(error)))
      .unwrap();
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
  fn test_rwlock() {
    let rwl = RwLock::new(5i32);
    let numb = aw!(rwl.read());
    println!("{}", *numb);
  }
}
