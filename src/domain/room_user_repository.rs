use std::str::FromStr;
use async_trait::async_trait;
use cassandra_cpp::*;
use chrono::Utc;
use uuid::Uuid;
use tokio::sync::Mutex;

use crate::domain::repository::{Repository, Utils};
use crate::model::room_user::RoomUser;

pub struct RoomUserRepository {
    pub(crate) cluster: Mutex<Cluster>
}

#[async_trait]
impl Repository for RoomUserRepository {

    async fn retrieve_session(&self) -> Result<Session> {
        let mut cluster_ = self.cluster.lock().await;
        cluster_.connect_async().await
    }
}

impl RoomUserRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.room_users (room_id, user_id, room_title, create_at) VALUES(?, ?, ?, ?)";

    const SELECT_BY_USER: &'static str = "SELECT room_id, user_id, room_title, create_at FROM chat_app.room_users WHERE user_id = ? ALLOW FILTERING";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.room_users WHERE room_id = ? AND user_id = ?";

    const DELETE_BY_ROOM: &'static str = "DELETE FROM chat_app.room_users WHERE room_id = ?";

    pub async fn create_room_users(&self, input: RoomUser) -> Option<RoomUser> {
        let mut statement = stmt!(Self::INSERT_QUERY);

        statement.bind_string(0, input.room_id.as_str()).ok();
        statement.bind_uuid(1, Utils::from_uuid_to_cass_uuid(input.user_id)).ok();
        statement.bind_string(2, input.room_id.as_str()).ok();
        statement.bind_int64(3, Utc::now().timestamp()).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Ok(_) => Some(input),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn load_by_userid(&self, user_id: Uuid, page: i32, size: i32) -> Option<Vec<RoomUser>> {
        let mut res = Vec::<RoomUser>::new();

        let mut has_more_pages = true;
        let mut paging = page - 1;

        let mut statement = Statement::new(Self::SELECT_BY_USER, 1);
        statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid(user_id)).ok();
        statement.set_paging_size(size).ok();

        let session = self.retrieve_session().await.unwrap();
        while has_more_pages && paging >= 0 {
            match session.execute(&statement).wait().ok() {
                None => break,
                Some(result) => {

                    if paging == 0 {
                        for row in result.iter() {
                            match Self::bind_to_roomuser(row) {
                                None => continue,
                                Some(user) => res.push(user),
                            };
                        }
                    }

                    has_more_pages = result.has_more_pages();
                    if has_more_pages {
                        statement.set_paging_state(result).ok();
                    }
                    paging -= 1;
                }
            };
        }

        Some(res)
    }

    pub async fn delete_room_user(&self, room_id: String, user_id: Uuid) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_QUERY);
        statement.bind_string(0, room_id.as_str()).ok();
        let cass_uuid = cassandra_cpp::Uuid::from_str( user_id.to_string().as_str() ).ok().unwrap();
        statement.bind_uuid(1, cass_uuid).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = Result::ok(session.execute(&statement).wait());

        match result {
            Some(_) => Ok(()),
            None => {
                Err(Error::from_kind(ErrorKind::Msg("Delete user failed".to_string())))
            }
        }
    }

    pub async fn delete_by_room(&self, room_id: String) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_BY_ROOM);
        statement.bind_string(0, room_id.as_str()).ok();
        let session = self.retrieve_session().await.unwrap();
        let result = Result::ok(session.execute(&statement).wait());

        match result {
            Some(_) => Ok(()),
            None => {
                Err(Error::from_kind(ErrorKind::Msg("Delete user failed".to_string())))
            }
        }
    }

    fn bind_to_roomuser(row: Row) -> Option<RoomUser> {
        let user_id: cassandra_cpp::Uuid = Result::ok( row.get(1) ).unwrap();
        Some(
            RoomUser {
                room_id: Result::ok(row.get(0)).unwrap(),
                user_id: Utils::from_cass_uuid_to_uuid(user_id),
                room_title: Result::ok(row.get(2)).unwrap(),
                create_at: Utils::from_timestamp_to_datetime(Result::ok(row.get(3)).unwrap()),
            }
        )
    }
}