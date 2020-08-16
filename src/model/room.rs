use crate::model::user::User;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct Room {
  pub room_id: String,
  pub host_info: User,
  pub participants: Option<Vec<User>>,
  pub create_at: DateTime<Utc>,
}