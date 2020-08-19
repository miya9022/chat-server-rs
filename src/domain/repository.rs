use std::sync::Arc;
use cassandra_cpp::*;
use crate::model::room::Room;
use crate::model::user::User;
use tokio::stream::StreamExt;
use uuid::Uuid;
use std::str::FromStr;

pub trait Repository {

    const SELECT_ALL_QUERY: &'static str;

    const SELECT_ONE_QUERY: &'static str;

    fn retrieve_session(&self) -> &Session;
}

pub struct RepositoryFactory {}

impl RepositoryFactory {
    pub fn new_repository(session: Session, kind: RepoKind) -> Box<dyn Repository>{
        match kind {
            RepoKind::ROOM => Box::new(RoomRepository {
                session: Arc::new(session)
            }),
            RepoKind::USER => Box::new(UserRepository {
                session: Arc::new(session)
            }),
            RepoKind::MESSAGE => Box::new(MessageRepository {
                session: Arc::new(session)
            }),
        }
    }
}

pub enum RepoKind {
    ROOM,
    USER,
    MESSAGE,
}

pub struct RoomRepository {
    session: Arc<Session>
}

impl Repository for RoomRepository {
    const SELECT_ALL_QUERY: &'static str = "SELECT room_id, host_id, host_name, participants, create_at, delete_key FROM chat_app.room";

    const SELECT_ONE_QUERY: &'static str = Self::SELECT_ALL_QUERY + " WHERE room_id = ?";

    fn retrieve_session(&self) -> &Session {
        &self.session
    }
}

impl RoomRepository {
    const SEPARATOR_CHARS: &'static str = "*&&*";

    pub async fn load_rooms(&self) -> Vec<Room> {
        let mut statement = stmt!(Self::SELECT_ALL_QUERY);
        let result = self.retrieve_session().execute(&statement).await;

        match result {
            Err(_) => vec!(),
            Ok(cass_result) => {
                cass_result.iter().map(|row| {
                    let participants: Vec<String> = row.get(3)?;
                    let participants = Self::get_participants(participants);

                    Room {
                        room_id: row.get(0)?,
                        host_info: User {
                            id: row.get(1)?,
                            name: row.get(2)?,
                        },
                        participants,
                        create_at: row.get(4)?,
                        delete_key: row.get(5)?,
                    }
                }).collect()
            }
        }
    }

    pub async fn load_one_room(&self, room_id: &str) -> Option<Room> {
        let mut statement = stmt!(Self::SELECT_ONE_QUERY);
        statement.bind_string(0, room_id)?;
        let result = self.retrieve_session().execute(&statement).await?;

        match result.first_row() {
            None => None,
            Some(row) => Self::bind_to_room(row)
        }
    }

    fn get_participants(participants: Vec<String>) -> Option<Vec<User>> {
        participants.iter().map(|participant| {
            let items: Vec<String> = participant.split(Self::SEPARATOR_CHARS).collect();
            User {
                id: Uuid::from_str(items.get(0).unwrap().as_str())?,
                name: String::from(items.get(1).unwrap().as_str()),
            }
        }).collect()
    }

    fn bind_to_room(row: Row) -> Option<Room> {
        Some(Room {
            room_id: row.get(0)?,
            host_info: User {
                id: row.get(1)?,
                name: row.get(2)?,
            },
            participants: Self::get_participants(row.get(3)?),
            create_at: row.get(4)?,
            delete_key: row.get(5)?,
        })
    }
}

pub struct UserRepository {
    session: Arc<Session>
}

impl Repository for UserRepository {
    const SELECT_ALL_QUERY: &'static str = "";
    const SELECT_ONE_QUERY: &'static str = "";

    fn retrieve_session(&self) -> &Session {
        &self.session
    }
}

pub struct MessageRepository {
    session: Arc<Session>
}

impl Repository for MessageRepository {
    const SELECT_ALL_QUERY: &'static str = "";
    const SELECT_ONE_QUERY: &'static str = "";

    fn retrieve_session(&self) -> &Session {
        &self.session
    }
}