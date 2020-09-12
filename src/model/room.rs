use uuid::Uuid;
use crate::model::user::User;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct Room {
  pub room_id: String,
  pub room_title: String,
  pub host_info: User,
  pub participants: Option<Vec<User>>,
  pub create_at: DateTime<Utc>,
  pub scope: String,
  pub delete_key: String,
}

impl Room {
  pub fn new(
    room_id: String,
    room_title: String,
    host_id: Uuid, 
    host_name: String, 
    participants: Option<Vec<User>>, 
    create_at: DateTime<Utc>,
    scope: String,
    delete_key: String,
  ) -> Self {
    Room {
      room_id,
      room_title,
      host_info: User::new(host_id, host_name.as_str()),
      participants,
      create_at,
      scope,
      delete_key
    }
  }
}