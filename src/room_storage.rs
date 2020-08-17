use futures::StreamExt;
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;
use chrono::Utc;
use tokio::sync::{ RwLock, broadcast, mpsc::UnboundedReceiver };

use crate::model::{room::Room, user::User};
use crate::hub::{Hub, HubOptions};
use crate::proto::{RoomInput, Input, InputParcel, Output, OutputParcel, OutputError, RoomCreatedOutput, RoomRemovedOutput};

const OUTPUT_CHANNEL_SIZE: usize = 65536;

pub struct RoomStorage {
  output_sender: broadcast::Sender<OutputParcel>,
  rooms: RwLock<HashMap<String, Room>>,
  hubs: RwLock<HashMap<String, Arc<Hub>>>,
  hub_options: Option<HubOptions>,
}

impl RoomStorage {
  pub fn new(hub_options: Option<HubOptions>) -> Self {
    let (output_sender, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
    RoomStorage {
      output_sender,
      rooms: Default::default(),
      hubs: Default::default(),
      hub_options,
    }
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

  pub async fn run(&self, receiver: UnboundedReceiver<InputParcel>) {
    let processing = receiver.for_each(|input_parcel| self.process(input_parcel));

    tokio::select! {
      _ = processing -> {}
    }
  }

  async fn process(&self, input_parcel: InputParcel) {
    match input_parcel.input {
      Input::CreateRoom(room_input) => self.create_room(input_parcel.room_id, room_input).await,
      Input::DeleteRoom(room_id) => self.delete_room(room_id).await,
      _ => match self.get_hub(input_parcel.room_id.as_str()).await {
        Some(hub) => {
          hub.process(input_parcel);
        },
        None => self.send_error(input_parcel.room_id.as_str(), OutputError::RoomNotExists)
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
      .filter(|parts| parts.len() > 0)
      .map(|parts| {
        (*parts).iter()
          .map(|part| {
            User::new(Uuid::new_v4(), part)
          })
          .collect()
      });

    let room = Room::new(room_id.clone(), input.host_id, input.host_name, users, Utc::now());
    self.rooms.write().await.insert(room_id.clone(), room.clone());

    // create Hub
    let hub = Hub::new(self.hub_options.unwrap(), &self.output_sender);
    self.hubs.write().await.insert(room_id.clone(), Arc::new(hub));

    // send created log
    self.output_sender
      .send(OutputParcel::new(room_id.clone(), Default::default(), Output::RoomCreated(RoomCreatedOutput::new(room_id))))
      .unwrap();
  }

  async fn delete_room(&self, room_id: String) {

    // if room_id not exists send room not exists log
    if !self.rooms.read().await.contains_key(room_id.as_str()) {
      self.send_error(room_id.as_str(), OutputError::RoomNotExists);
      return;
    }

    // if room_id exists, delete room instance
    self.rooms.write().await.remove(room_id.as_str()).unwrap();
    let hub = self.hubs.write().await.remove(room_id.as_str()).unwrap();
    drop(hub);

    self.output_sender
      .send(OutputParcel::new(room_id.clone(), Default::default(), Output::RoomRemoved(RoomRemovedOutput::new(room_id))))
      .unwrap();
  }

  fn send_error(&self, room_id: &str, error: OutputError) {
    self.output_sender
      .send(OutputParcel::new(String::from(room_id), Default::default(), Output::Error(error)))
      .unwrap();
  }
}