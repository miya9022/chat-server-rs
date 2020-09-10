use std::{error, result};

use futures::stream::SplitStream;
use futures::{future, Stream, StreamExt, TryStream, TryStreamExt};
use uuid::Uuid;
use warp::filters::ws::WebSocket;

use crate::error::{Error, Result};
use crate::proto::{InputParcel, OutputParcel, Input};
use crate::proto::Input::{PostMessage, JoinRoom};
use std::str::FromStr;

#[derive(Clone, Default)]
pub struct RoomClient {
  pub id: Uuid,
  pub room_id: String,
}

impl RoomClient {
  pub fn new(room_id: String) -> Self {
    RoomClient { room_id, id: Uuid::default() }
  }

  pub fn read_input(
    &self,
    stream: SplitStream<WebSocket>
  ) -> impl Stream<Item = Result<InputParcel>> {
    let rid = self.room_id.clone();
    let mut client_id = self.id;

    stream
      // take only text messages
      .take_while(|message| {
        future::ready(if let Ok(message) = message {
          message.is_ping() || message.is_text()
        } else {
          false
        })
      })
      // deserialize json
      .map(move |message| match message {
        Err(err) => Err(Error::System(err.to_string())),
        Ok(message) => {
          if message.is_text() {
            let input: Input = serde_json::from_str(message.to_str().unwrap())?;
            if let PostMessage(post) = input.clone() {
              client_id = post.client_id;
            }

            if let JoinRoom(join) = input.clone() {
              client_id = join.client_id;
            }
            Ok(InputParcel::new(client_id, rid.clone(), input))
          }
          else if message.is_ping() {
            Ok(InputParcel::new(client_id, rid.clone(), Input::Ping))
          }
          else {
            Err(Error::System("UNDEFINED BEHAVIOR".to_string()))
          }

        }
      })
  }

  pub fn write_output<S, E>(&self, stream: S) -> impl Stream<Item = Result<warp::ws::Message>>
  where 
    S: TryStream<Ok = OutputParcel, Error = E> + Stream<Item = result::Result<OutputParcel, E>>,
    E: error::Error,
  {
    let room_id = self.room_id.clone();
    stream
      // skip irrelevent parcels
      .try_filter(move |output_parcel| {
        future::ready(output_parcel.room_id == room_id)
      })
      // serialize to JSON
      .map_ok(|output_parcel| {
        let data = serde_json::to_string(&output_parcel.output).unwrap();
        warp::ws::Message::text(data)
      })
      .map_err(|err| Error::System(err.to_string()))
  }
}

#[derive(Clone, Default)]
pub struct UserClient {
  pub id: Uuid,
}

impl UserClient {
  pub fn new(user_id: String) -> Self {
    UserClient { id: Uuid::from_str(user_id.as_str()).unwrap() }
  }

  pub fn read_input(
    &self,
    stream: SplitStream<WebSocket>
  ) -> impl Stream<Item = Result<InputParcel>> {
    let client_id = self.id;

    stream
        // take only text messages
        .take_while(|message| {
          future::ready(if let Ok(message) = message {
            message.is_ping() || message.is_text()
          } else {
            false
          })
        })
        // deserialize json
        .map(move |message| match message {
          Err(err) => Err(Error::System(err.to_string())),
          Ok(message) => {
            if message.is_text() {
              let input: Input = serde_json::from_str(message.to_str().unwrap())?;
              Ok(InputParcel::new(client_id, "".to_string(), input))
            }
            else if message.is_ping() {
              Ok(InputParcel::new(client_id, "".to_string(), Input::Ping))
            }
            else {
              Err(Error::System("UNDEFINED BEHAVIOR".to_string()))
            }
          }
        })
  }

  pub fn write_output<S, E>(&self, stream: S) -> impl Stream<Item = Result<warp::ws::Message>>
    where
        S: TryStream<Ok = OutputParcel, Error = E> + Stream<Item = result::Result<OutputParcel, E>>,
        E: error::Error,
  {
    let user_id = self.id;
    stream
        // skip irrelevent parcels
        .try_filter(move |output_parcel| {
          future::ready(output_parcel.client_id == user_id)
        })
        // serialize to JSON
        .map_ok(|output_parcel| {
          let data = serde_json::to_string(&output_parcel.output).unwrap();
          warp::ws::Message::text(data)
        })
        .map_err(|err| Error::System(err.to_string()))
  }
}