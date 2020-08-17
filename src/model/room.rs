use uuid::Uuid;
use crate::model::user::User;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct Room {
  pub room_id: String,
  pub host_info: User,
  pub participants: Option<Vec<User>>,
  pub create_at: DateTime<Utc>,
}

impl Room {
  pub fn new(room_id: String, host_id: Uuid, host_name: String, participants: Option<Vec<User>>, create_at: DateTime<Utc>) -> Self {
    Room {
      room_id,
      host_info: User::new(host_id, host_name.as_str()),
      participants,
      create_at,
    }
  }
}