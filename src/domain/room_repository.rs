use std::str::FromStr;
use async_trait::async_trait;
use cassandra_cpp::*;
use chrono::{NaiveDateTime, DateTime, Utc};
use uuid::Uuid;
use tokio::sync::Mutex;

use crate::model::room::Room;
use crate::model::user::User;
use crate::domain::repository::{Repository, Utils};

pub struct RoomRepository {
    pub(crate) cluster: Mutex<Cluster>
}

#[async_trait]
impl Repository for RoomRepository {

    async fn retrieve_session(&self) -> Result<Session> {
        let mut cluster_ = self.cluster.lock().await;
        cluster_.connect_async().await
    }
}

impl RoomRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.room (room_id, room_title, host_id, host_name, participants, create_at, scope, delete_key) \
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

    const UPDATE_PARTICIPANTS_QUERY: &'static str = "UPDATE chat_app.room SET participants = ? WHERE room_id = ?";

    const SELECT_ALL_QUERY: &'static str = "SELECT room_id, room_title, host_id, host_name, participants, create_at, scope, delete_key FROM chat_app.room";

    const SELECT_ONE_QUERY: &'static str = "SELECT room_id, room_title, host_id, host_name, participants, create_at, scope, delete_key FROM chat_app.room \
    WHERE room_id = ?";

    const SELECT_EXISTS_QUERY: &'static str = "SELECT COUNT(*) FROM chat_app.room WHERE room_id = ?";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.room WHERE room_id = ?";

    pub async fn create_room(&self, room: Room) -> Option<Room> {
        let persistence_room = room.clone();
        let mut statement = stmt!(Self::INSERT_QUERY);
        statement.bind_string(0, persistence_room.room_id.as_str()).ok();
        let host_id = cassandra_cpp::Uuid::from_str(
            persistence_room.host_info.id.to_string().as_str()
        ).ok().unwrap();
        statement.bind_string(1, persistence_room.room_title.as_str()).ok();
        statement.bind_uuid(2, host_id).ok();
        statement.bind_string(3, persistence_room.host_info.name.as_str()).ok();

        let participants_set = match persistence_room.participants {
            Some(participants) => {
                let mut set = Set::new(participants.len());
                for item in participants {
                    let value = item.id.to_string() + Utils::SEPARATOR_CHARS + item.name.as_str();
                    match set.append_string(value.as_str()) {
                        Ok(rs) => {
                            println!("{:?}", rs);
                        }
                        Err(error) => {
                            println!("{:?}", error);
                        }
                    }
                }
                set
            }
            None => Set::new_from_data_type(DataType::new(ValueType::VARCHAR), 0),
        };
        statement.bind_set(4, participants_set).ok();
        statement.bind_int64(5, persistence_room.create_at.timestamp()).ok();
        statement.bind_string(6, persistence_room.scope.as_str()).ok();
        statement.bind_string(7, persistence_room.delete_key.as_str()).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Ok(_) => Some(room),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn update_participant_in_room(&self, room_id: &str, participants: Vec<User>) -> Result<()> {
        if self.load_one_room(room_id).await.is_none() {
            println!("Room not found");
            return Ok(());
        }

        let mut statement = stmt!(Self::UPDATE_PARTICIPANTS_QUERY);
        let mut set = Set::new_from_data_type(DataType::new(ValueType::VARCHAR), participants.len());
        participants.iter().for_each(|item| {
            set.append_string(&(item.id.to_string() + Utils::SEPARATOR_CHARS + item.name.as_str())).ok();
        });
        statement.bind_set(0, set).ok();
        statement.bind_string(1, room_id).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                Err(error)
            }
        }
    }

    pub async fn load_rooms(&self) -> Vec<Room> {
        let statement = stmt!(Self::SELECT_ALL_QUERY);
        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Err(_) => vec!(),
            Ok(cass_result) => {
                cass_result.iter().map(|row| {
                    let participants: SetIterator = Result::ok(row.get(4)).unwrap();
                    let participants = Utils::get_participants(participants);

                    let host_id: cassandra_cpp::Uuid = Result::ok(row.get(2)).unwrap();
                    let create_at: i64 = Result::ok(row.get(5)).unwrap();
                    let create_at = NaiveDateTime::from_timestamp(create_at, 0);
                    let create_at = DateTime::from_utc(create_at, Utc);
                    Room {
                        room_id: Result::ok(row.get(0)).unwrap(),
                        room_title: Result::ok(row.get(1)).unwrap(),
                        host_info: User {
                            id: Utils::from_cass_uuid_to_uuid(host_id),
                            name: Result::ok(row.get(3)).unwrap(),
                        },
                        participants,
                        create_at,
                        scope: Result::ok( row.get(6) ).unwrap(),
                        delete_key: Result::ok(row.get(7)).unwrap(),
                    }
                }).collect()
            }
        }
    }

    pub async fn load_one_room(&self, room_id: &str) -> Option<Room> {
        let mut statement = stmt!(Self::SELECT_ONE_QUERY);
        statement.bind_string(0, room_id).ok();
        let session = self.retrieve_session().await.unwrap();
        let result = Result::ok(session.execute(&statement).wait()).unwrap();

        match result.first_row() {
            None => None,
            Some(row) => {
                Self::bind_to_room(row)
            }
        }
    }

    pub async fn room_exists(&self, room_id: &str) -> bool {
        let mut statement = stmt!(Self::SELECT_EXISTS_QUERY);
        statement.bind_string(0, room_id).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Err(error) => {
                println!("{:?}", error);
                false
            },
            Ok(result) => {
                result
                    .first_row().unwrap()
                    .get_column(0).unwrap()
                    .get_i64().unwrap()
                    > 0
            }
        }
    }

    pub async fn delete_room(&self, room_id: &str) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_QUERY);
        statement.bind_string(0, room_id).ok();
        let session = self.retrieve_session().await.unwrap();
        let result = Result::ok(session.execute(&statement).wait());

        match result {
            Some(_) => Ok(()),
            None => {
                Err(Error::from_kind(ErrorKind::Msg("Delete room failed".to_string())))
            }
        }
    }

    fn bind_to_room(row: Row) -> Option<Room> {
        let host_id: cassandra_cpp::Uuid = Result::ok(row.get(2)).unwrap();
        let participants: SetIterator = Result::ok(row.get(4)).unwrap();
        let create_at: i64 = Result::ok(row.get(5)).unwrap();
        let create_at = NaiveDateTime::from_timestamp(create_at, 0);
        let create_at = DateTime::from_utc(create_at, Utc);

        Some(Room {
            room_id: Result::ok(row.get(0)).unwrap(),
            room_title: Result::ok( row.get(1)).unwrap(),
            host_info: User {
                id: Uuid::from_str(host_id.to_string().as_str()).unwrap(),
                name: Result::ok(row.get(3)).unwrap(),
            },
            participants: Utils::get_participants(participants),
            create_at,
            scope: Result::ok( row.get(6) ).unwrap(),
            delete_key: Result::ok(row.get(7) ).unwrap(),
        })
    }
}