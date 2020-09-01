use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct RoomUser {
    pub room_id: String,
    pub user_id: Uuid,
    pub room_title: String,
    pub create_at: DateTime<Utc>,
}

impl RoomUser {
    pub fn new(
        room_id: String,
        room_title: String,
        user_id: Uuid,
        create_at: DateTime<Utc>,
    ) -> Self {
        RoomUser {
            room_id,
            room_title,
            user_id,
            create_at,
        }
    }
}