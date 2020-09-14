use crate::model::user::User;
use uuid::Uuid;
use chrono::Utc;

#[derive(Debug, Clone)]
pub struct Message {
  pub id: Uuid,

  pub from: User,
  pub room_id: String,
  pub body: String,
  pub create_at: i64,
}

impl Message {
  pub fn new(from: User, room_id: &str, body: &str) -> Self {
    Message {
      id: Uuid::default(),
      from,
      room_id: String::from(room_id),
      body: String::from(body),
      create_at: Utc::now().timestamp(),
    }
  }

  pub fn new_create_at(from: User, room_id: &str, body: &str, create_at: i64) -> Self {
    Message {
      id: Uuid::default(),
      from,
      room_id: String::from(room_id),
      body: String::from(body),
      create_at,
    }
  }
}