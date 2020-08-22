use std::sync::Arc;
use cassandra_cpp::*;
use crate::model::room::Room;
use crate::model::user::User;
use uuid::Uuid;
use std::str::FromStr;
use chrono::{NaiveDateTime, DateTime, Utc};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Default)]
pub struct RepositoryFactory(HashMap<String, Rc<dyn Repository>>);

impl RepositoryFactory {

    pub fn new() -> Self {
        RepositoryFactory(HashMap::<String, Rc<dyn Repository>>::new())
    }

    pub fn add_repository(&mut self, session: &mut Session, kind: RepoKind) {
        let (key, repo): (&str, Rc<dyn Repository>) = match kind {
            RepoKind::ROOM => ("ROOM", Rc::new(RoomRepository {
                session: unsafe { Arc::from_raw(session) }
            })),
            RepoKind::USER => ("USER", Rc::new(UserRepository {
                session: unsafe { Arc::from_raw(session) }
            })),
            RepoKind::MESSAGE => ("MESSAGE", Rc::new(MessageRepository {
                session: unsafe { Arc::from_raw(session) }
            })),
        };
        self.0.insert(key.to_string(), repo);
    }

    pub fn get_repository(&self, repo_name: &str) -> Rc<dyn Repository> {
        match self.0.get(repo_name) {
            Some(repo) => Rc::clone(repo),
            None => panic!("Not supported")
        }
    }
}

pub enum RepoKind {
    ROOM,
    USER,
    MESSAGE,
}

pub trait Repository {

    fn retrieve_session(&self) -> &Session;
}

pub struct RoomRepository {
    session: Arc<Session>
}

impl Repository for RoomRepository {

    fn retrieve_session(&self) -> &Session {
        // TODO: check session is closed, create new session
        &self.session
    }
}

impl RoomRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.room (room_id, host_id, host_name, participants, create_at, delete_key) \
    VALUES(?, ?, ?, ?, ?, ?)";

    const UPDATE_PARTICIPANTS_QUERY: &'static str = "UPDATE chat_app.room SET participants = ? WHERE room_id = ?";

    const SELECT_ALL_QUERY: &'static str = "SELECT room_id, host_id, host_name, participants, create_at, delete_key FROM chat_app.room";

    const SELECT_ONE_QUERY: &'static str = "SELECT room_id, host_id, host_name, participants, create_at, delete_key FROM chat_app.room \
    WHERE room_id = ?";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.room WHERE room_id = ?";

    const SEPARATOR_CHARS: &'static str = "*&&*";

    pub async fn create_room(&self, room: Room) -> Option<Room> {
        let persistence_room = room.clone();
        let mut statement = stmt!(Self::INSERT_QUERY);
        statement.bind_string(0, persistence_room.room_id.as_str()).ok();
        let host_id = cassandra_cpp::Uuid::from_str(
            persistence_room.host_info.id.to_string().as_str()
            )
            .ok().unwrap();
        statement.bind_uuid(1, host_id).ok();
        statement.bind_string(2, persistence_room.host_info.name.as_str()).ok();

        let participants_set = match persistence_room.participants {
            Some(participants) => {
                let mut set = Set::new_from_data_type(DataType::new(ValueType::VARCHAR), participants.len());
                participants.iter().for_each(|item| {
                    set.append_string(&(item.id.to_string() + Self::SEPARATOR_CHARS + item.name.as_str())).ok();
                });
                set
            }
            None => Set::new_from_data_type(DataType::new(ValueType::VARCHAR), 0),
        };
        statement.bind_set(3, participants_set).ok();
        statement.bind_int64(4, persistence_room.create_at.timestamp()).ok();
        statement.bind_string(5, persistence_room.delete_key.as_str()).ok();

        let result = self.retrieve_session().execute(&statement).await;
        match result {
            Ok(_) => Some(room),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn update_participant_in_room(&self, room_id: &str, participants: Vec<User>) -> Result<()> {
        let mut statement = stmt!(Self::UPDATE_PARTICIPANTS_QUERY);
        let mut set = Set::new_from_data_type(DataType::new(ValueType::VARCHAR), participants.len());
        participants.iter().for_each(|item| {
            set.append_string(&(item.id.to_string() + Self::SEPARATOR_CHARS + item.name.as_str())).ok();
        });
        statement.bind_set(0, set).ok();
        statement.bind_string(1, room_id).ok();

        let result = self.retrieve_session().execute(&statement).await;
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
        let result = self.retrieve_session().execute(&statement).await;

        match result {
            Err(_) => vec!(),
            Ok(cass_result) => {
                cass_result.iter().map(|row| {
                    let participants: SetIterator = Result::ok(row.get(3)).unwrap();
                    let participants = Self::get_participants(participants);

                    let host_id: String = Result::ok(row.get(1)).unwrap();
                    let create_at: i64 = Result::ok(row.get(4)).unwrap();
                    let create_at = NaiveDateTime::from_timestamp(create_at, 0);
                    let create_at = DateTime::from_utc(create_at, Utc);
                    Room {
                        room_id: Result::ok(row.get(0)).unwrap(),
                        host_info: User {
                            id: Uuid::from_str(host_id.as_str()).unwrap(),
                            name: Result::ok(row.get(2)).unwrap(),
                        },
                        participants,
                        create_at,
                        delete_key: Result::ok(row.get(5)).unwrap(),
                    }
                }).collect()
            }
        }
    }

    pub async fn load_one_room(&self, room_id: &str) -> Option<Room> {
        let mut statement = stmt!(Self::SELECT_ONE_QUERY);
        statement.bind_string(0, room_id).ok();
        let result = Result::ok(self.retrieve_session().execute(&statement).await).unwrap();

        match result.first_row() {
            None => None,
            Some(row) => Self::bind_to_room(row)
        }
    }

    pub async fn delete_room(&self, room_id: &str) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_QUERY);
        statement.bind_string(0, room_id).ok();
        let result = Result::ok(self.retrieve_session().execute(&statement).await);

        match result {
            Some(_) => Ok(()),
            None => {
                Err(Error::from_kind(ErrorKind::Msg("Delete room failed".to_string())))
            }
        }
    }

    fn get_participants(participants: SetIterator) -> Option<Vec<User>> {
        Some(
            participants.map(|participant| {
                let part_record = Result::ok(participant.get_string()).unwrap();
                let items: Vec<&str> = part_record.split(Self::SEPARATOR_CHARS).collect();
                User {
                    id: Uuid::from_str(items.get(0).unwrap()).unwrap(),
                    name: items.get(1).unwrap().to_string(),
                }
            }).collect()
        )
    }

    fn bind_to_room(row: Row) -> Option<Room> {
        let host_id: cassandra_cpp::Uuid = Result::ok(row.get(1)).unwrap();
        let participants: SetIterator = Result::ok(row.get(3)).unwrap();
        let create_at: i64 = Result::ok(row.get(4)).unwrap();
        let create_at = NaiveDateTime::from_timestamp(create_at, 0);
        let create_at = DateTime::from_utc(create_at, Utc);

        Some(Room {
            room_id: Result::ok(row.get(0)).unwrap(),
            host_info: User {
                id: Uuid::from_str(host_id.to_string().as_str()).unwrap(),
                name: Result::ok(row.get(2)).unwrap(),
            },
            participants: Self::get_participants(participants),
            create_at,
            delete_key: Result::ok(row.get(5)).unwrap(),
        })
    }
}

pub struct UserRepository {
    session: Arc<Session>
}

impl Repository for UserRepository {

    fn retrieve_session(&self) -> &Session {
        &self.session
    }
}

impl UserRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.user (id, name, create_at) VALUES(?, ?, ?)";

    const SELECT_ALL_QUERY: &'static str = "SELECT id, name FROM chat_app.user ORDER BY create_at DESC";

    const SELECT_ONE_QUERY: &'static str = "SELECT id, name FROM chat_app.user WHERE id = ?";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.user WHERE id = ?";

    pub async fn create_user(&self, user: User) -> Option<User> {
        let persistent_user = user.clone();
        let mut statement = stmt!(Self::INSERT_QUERY);
        let user_id = cassandra_cpp::Uuid::from_str(
            persistent_user.id.to_string().as_str()
        )
        .ok().unwrap();

        statement.bind_uuid(0, user_id).ok();
        statement.bind_string(1, persistent_user.name.as_str()).ok();
        statement.bind_int64(2, Utc::now().timestamp()).ok();

        let result = self.retrieve_session().execute(&statement).await;
        match result {
            Ok(_) => Some(user),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn load_users(&self, page: i32, size: i32) -> Option<Vec<User>> {
        let mut res = Vec::<User>::new();

        let mut has_more_pages = true;
        let mut paging = page - 1;

        let mut statement = Statement::new(Self::SELECT_ALL_QUERY, 0);
        statement.set_paging_size(size).ok();

        while has_more_pages && paging >= 0 {
            match self.retrieve_session().execute(&statement).await.ok() {
                None => break,
                Some(result) => {

                    if paging == 0 {
                        for row in result.iter() {
                            match Self::bind_to_user(row) {
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

    pub async fn load_one_user(&self, id: Uuid) -> Option<User> {
        let mut statement = stmt!(Self::SELECT_ONE_QUERY);
        statement.bind_uuid(0, cassandra_cpp::Uuid::from_str( id.to_string().as_str() )
            .unwrap()).ok();

        match self.retrieve_session().execute(&statement).await {
            Err(error) => {
                println!("{:?}", error);
                None
            },
            Ok(result) => {
                match result.first_row() {
                    None => None,
                    Some(row) => Self::bind_to_user(row)
                }
            }
        }
    }

    pub async fn delete_user(&self, id: Uuid) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_QUERY);
        let cass_uuid = cassandra_cpp::Uuid::from_str( id.to_string().as_str() ).ok().unwrap();
        statement.bind_uuid(0, cass_uuid).ok();

        let result = Result::ok(self.retrieve_session().execute(&statement).await);

        match result {
            Some(_) => Ok(()),
            None => {
                Err(Error::from_kind(ErrorKind::Msg("Delete user failed".to_string())))
            }
        }
    }

    fn bind_to_user(row: Row) -> Option<User> {
        let user_id: cassandra_cpp::Uuid = Result::ok( row.get(0) ).unwrap();
        Some(
            User {
                id: Uuid::from_str( user_id.to_string().as_str() ).unwrap(),
                name: Result::ok( row.get(1) ).unwrap(),
            }
        )
    }
}

pub struct MessageRepository {
    session: Arc<Session>
}

impl Repository for MessageRepository {

    fn retrieve_session(&self) -> &Session {
        &self.session
    }
}

impl MessageRepository {

}