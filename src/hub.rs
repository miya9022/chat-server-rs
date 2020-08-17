use futures::StreamExt;
use uuid::Uuid;
use regex::Regex;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{self, Duration};
use tokio::sync::{broadcast, RwLock};
use chrono::Utc;

use crate::proto::*;
use crate::model::{user::User, feed::Feed, message::Message};

const OUTPUT_CHANNEL_SIZE: usize = 16;
const MAX_MESSAGE_BODY_LENGTH: usize = 256;
lazy_static! {
  static ref USER_NAME_REGEX: Regex = Regex::new("[A-Za-z\\s]{4,24}").unwrap();
}

#[derive(Clone, Copy, Default)]
pub struct HubOptions {
  pub alive_interval: Option<Duration>,
}

pub struct Hub {
  alive_interval: Option<Duration>,
  output_sender: broadcast::Sender<OutputParcel>,
  users: RwLock<HashMap<Uuid, User>>,
  feed: RwLock<Feed>,
}

impl Hub {
  pub fn new<'a>(options: HubOptions, output_sender: &'a broadcast::Sender<OutputParcel>) -> Self {
    // let (output_sender, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
    Hub {
      alive_interval: options.alive_interval,
      output_sender: *output_sender,
      users: Default::default(),
      feed: Default::default(),
    }
  }

  // async fn send(&self, room_id: &str, output: Output) {
  //   if self.output_sender.receiver_count() == 0 {
  //     return;
  //   }

  //   self.users.read().await.keys().for_each(|user_id| {
  //     self.output_sender
  //       .send(OutputParcel::new(String::from(room_id), *user_id, output.clone()))
  //       .unwrap();
  //   });
  // }

  fn send_targeted(&self, room_id: &str, client_id: Uuid, output: Output) {
    if self.output_sender.receiver_count() > 0 {
      self.output_sender
        .send(OutputParcel::new(String::from(room_id), client_id, output))
        .unwrap();
    }
  }

  async fn send_ignored(&self, room_id: &str, ignore_client_id: Uuid, output: Output) {
    if self.output_sender.receiver_count() == 0 {
      return;
    }

    self.users
      .read()
      .await
      .values()
      .filter(|user| user.id != ignore_client_id)
      .for_each(|user| {
        self.output_sender
          .send(OutputParcel::new(String::from(room_id), user.id, output.clone()))
          .unwrap();
      });
  }

  fn send_error(&self, room_id: &str, client_id: Uuid, error: OutputError) {
    self.send_targeted(room_id, client_id, Output::Error(error));
  }

  pub fn subscribe(&self) -> broadcast::Receiver<OutputParcel> {
    self.output_sender.subscribe()
  }

  pub async fn on_disconnect(&self, room_id: &str, client_id: Uuid) {
    if self.users.write().await.remove(&client_id).is_some() {
      self.send_ignored(room_id, client_id, Output::UserLeft(UserLeftOutput::new(client_id))).await
    }
  }

  // async fn tick_alive(&self, room_id: &str) {
  //   let alive_interval = if let Some(alive_interval) = self.alive_interval {
  //     alive_interval
  //   } else {
  //     return;
  //   };

  //   loop {
  //     time::delay_for(alive_interval).await;
  //     self.send(room_id, Output::Alive).await;
  //   }
  // }

  // pub async fn run(&self, receiver: UnboundedReceiver<InputParcel>) {
  //   let ticking_alive = self.tick_alive();
  //   let processing = receiver.for_each(|input_parcel| self.process(input_parcel));

  //   tokio::select! {
  //     _ = ticking_alive => {},
  //     _ = processing => {},
  //   }
  // }
  
  pub async fn process(&self, input_parcel: InputParcel) {
    match input_parcel.input {
      Input::JoinRoom(input) => self.process_join(input_parcel.room_id.as_str(), input_parcel.client_id, input).await,
      Input::PostMessage(input) => self.process_post(input_parcel.room_id.as_str(), input_parcel.client_id, input).await,
      _ => unimplemented!(),
    }
  }

  async fn process_join(&self, room_id: &str, client_id: Uuid, input: JoinInput) {
    let user_name = input.name.trim();

    // check if user name is taken
    if self
      .users
      .read()
      .await
      .values()
      .any(|user| user.name == user_name)
    {
      self.send_error(room_id, client_id, OutputError::UserNameTaken);
      return;
    }

    // Validate username
    if !USER_NAME_REGEX.is_match(user_name) {
      self.send_error(room_id, client_id, OutputError::InvalidUserName);
      return;
    }

    let user = User::new(client_id, user_name);
    self.users.write().await.insert(client_id, user.clone());

    // report success
    let user_output = UserOutput::new(client_id, user_name);
    let other_users = self
      .users
      .read()
      .await
      .values()
      .filter_map(|user| {
        if user.id != client_id {
          Some(UserOutput::new(user.id, &user.name))
        } else {
          None
        }
      })
      .collect();

    let messages = self
      .feed
      .read()
      .await
      .messages_iter()
      .map(|message| {
        MessageOutput::new(
          message.id,
          UserOutput::new(message.user.id, &message.user.name),
          &message.body,
          message.created_at,
        )
      })
      .collect();

    self.send_targeted(
      room_id,
      client_id,
      Output::Joined(
        JoinedOutput::new(user_output.clone(), other_users, messages),
      ));
    
    // notify others that someone joined
    self.send_ignored(
      room_id,
      client_id,
      Output::UserJoined(UserJoinedOutput::new(user_output))
    )
    .await;
  }

  async fn process_post(&self, room_id: &str, client_id: Uuid, input: PostInput) {
    // verify that user exists
    let user = if let Some(user) = self.users.read().await.get(&client_id) {
      user.clone()
    } else {
      self.send_error(room_id, client_id, OutputError::UserNotJoined);
      return;
    };

    // validate message body
    if input.body.is_empty() || input.body.len() > MAX_MESSAGE_BODY_LENGTH {
      self.send_error(room_id, client_id, OutputError::InvalidMessageBody);
      return;
    }

    // Add new message to feed
    let message = Message::new(Uuid::new_v4(), user.clone(), &input.body, Utc::now());
    self.feed.write().await.add_message(message.clone());

    // report send message success
    let message_output = MessageOutput::new(
      message.id, UserOutput::new(user.id, &user.name), &message.body, message.created_at
    );

    // report post status
    self.send_targeted(room_id, client_id, Output::Posted(PostedOutput::new(message_output.clone())));

    // notify everyone about new message
    self.send_ignored(room_id, client_id, Output::UserPosted(UserPostedOutput::new(message_output)))
    .await;
  }
}