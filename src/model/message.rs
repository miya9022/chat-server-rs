use crate::model::user::User;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Message {
  pub id: Uuid,

  pub from: User,
  pub to: User,

  pub room_id: String,
  pub body: String,
}

impl Message {
  pub fn new(from: User, to: User, room_id: &str, body: &str) -> Self {
    Message {
      id: Uuid::default(),
      from,
      to,
      room_id: String::from(room_id),
      body: String::from(body),
    }
  }
}