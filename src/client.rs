use std::{error, result};

use futures::stream::SplitStream;
use futures::{future, Stream, StreamExt, TryStream, TryStreamExt};
use uuid::Uuid;
use warp::filters::ws::WebSocket;

use crate::error::{Error, Result};
use crate::proto::{InputParcel, OutputParcel};

#[derive(Clone, Default)]
pub struct RoomClient {
  pub room_id: String,
}

impl RoomClient {
  pub fn new(room_id: &str) -> Self {
    RoomClient {
      room_id: String::from(room_id),
    }
  }
}

#[derive(Clone, Default)]
pub struct ChatClient {
  pub id: Uuid,
  pub room_id: String,
}

impl ChatClient {
  pub fn new(room_id: &str) -> Self {
    ChatClient { id: Uuid::new_v4(), room_id: String::from(room_id) }
  }

  pub fn read_input(
    &self,
    stream: SplitStream<WebSocket>
  ) -> impl Stream<Item = Result<InputParcel>> {
    let client_id = self.id;
    let rid = self.room_id.clone();

    stream
      // take only text messages
      .take_while(|message| {
        future::ready(if let Ok(message) = message {
          message.is_text()
        } else {
          false
        })
      })
      // deserialize json
      .map(move |message| match message {
        Err(err) => Err(Error::System(err.to_string())),
        Ok(message) => {
          let input = serde_json::from_str(message.to_str().unwrap())?;
          Ok(InputParcel::new(client_id, rid.clone(), input))
        }
      })
  }

  pub fn write_output<S, E>(&self, stream: S) -> impl Stream<Item = Result<warp::ws::Message>>
  where 
    S: TryStream<Ok = OutputParcel, Error = E> + Stream<Item = result::Result<OutputParcel, E>>,
    E: error::Error,
  {
    let client_id = self.id;
    stream
      // skip irrelevent parcels
      .try_filter(move |output_parcel| future::ready(output_parcel.client_id == client_id))
      // serialize to JSON
      .map_ok(|output_parcel| {
        let data = serde_json::to_string(&output_parcel.output).unwrap();
        warp::ws::Message::text(data)
      })
      .map_err(|err| Error::System(err.to_string()))
  }
}