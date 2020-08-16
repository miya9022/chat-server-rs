use futures::StreamExt;
use std::collections::HashMap;
use tokio::sync::{ RwLock, broadcast, mpsc::UnboundedReceiver };

use crate::model::room::Room;
use crate::hub::{Hub, HubOptions};
use crate::proto::{HostRoomInput, RoomInput, RoomOutputParcel, RoomInputParcel};

const OUTPUT_CHANNEL_SIZE: usize = 65536;

pub struct RoomStorage {
  output_sender: broadcast::Sender<RoomOutputParcel>,
  rooms: RwLock<HashMap<String, Room>>,
  hubs: RwLock<HashMap<String, Hub>>,
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

  async fn get_hub(&self, room_id: String) {

  }

  pub async fn run(&self, receiver: UnboundedReceiver<RoomInputParcel>) {
    let processing = receiver.for_each(|input_parcel| self.process(input_parcel));

    tokio::select! {
      _ = processing -> {}
    }
  }

  async fn process(&self, input_parcel: RoomInputParcel) {
    match input_parcel.input {
      HostRoomInput::CreateRoom(room_input) => self.create_room(input_parcel.room_id, room_input).await,
      HostRoomInput::DeleteRoom(room_id) => self.delete_room(room_id).await
    }
  }

  async fn create_room(&self, room_id: String, input: RoomInput) {

    // check room_id exits

    // check room_id valid

    // setup and serve Room instance 

    // create Hub

    // send created log

  }

  async fn delete_room(&self, room_id: String) {

    // if room_id not exists send room not exists log

    // if room_id exists, delete room instance
  }

}