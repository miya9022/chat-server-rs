use std::str::FromStr;
use async_trait::async_trait;
use cassandra_cpp::*;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use tokio::sync::Mutex;

use crate::model::user::User;
use crate::domain::repository::{Repository, Utils};

pub struct UserRepository {
    pub(crate) cluster: Mutex<Cluster>
}

#[async_trait]
impl Repository for UserRepository {

    async fn retrieve_session(&self) -> Result<Session> {
        let mut cluster_ = self.cluster.lock().await;
        cluster_.connect_async().await
    }
}

impl UserRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.user (id, name, create_at) VALUES(?, ?, ?)";

    const SELECT_ALL_QUERY: &'static str = "SELECT id, name FROM chat_app.user ORDER BY create_at DESC";

    const SELECT_ONE_QUERY: &'static str = "SELECT id, name FROM chat_app.user WHERE id = ?";

    const SELECT_HOST_USER: &'static str = "SELECT host_id, host_name FROM chat_app.room WHERE room_id = ?";

    const SELECT_PARTICIPANTS: &'static str = "SELECT participants FROM chat_app.room WHERE room_id = ?";

    const SELECT_EXISTS_QUERY: &'static str = "SELECT COUNT(*) FROM chat_app.user WHERE name = ? ALLOW FILTERING";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.user WHERE id = ?";

    pub async fn create_user(&self, user: User) -> Option<User> {
        let persistent_user = user.clone();
        println!("user info: {}, {}", user.id, user.name);
        if self.user_exists(user.clone()).await {
            println!("User has already exists");
            return None;
        }

        let mut statement = stmt!(Self::INSERT_QUERY);

        statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid(persistent_user.id)).ok();
        statement.bind_string(1, persistent_user.name.as_str()).ok();
        statement.bind_int64(2, Utc::now().timestamp()).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Ok(_) => {
                Some(user)
            },
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

        let session = self.retrieve_session().await.unwrap();
        while has_more_pages && paging >= 0 {
            match session.execute(&statement).wait().ok() {
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
        statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid( id )).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
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

    pub async fn load_host_room(&self, room_id: &str) -> Option<User> {
        let mut statement = stmt!(Self::SELECT_HOST_USER);
        statement.bind_string(0, room_id).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Err(_) => None,
            Ok(cass_result) => {
                if let Some(row) = cass_result.first_row() {
                    let host_id: cassandra_cpp::Uuid = Result::ok(row.get(0)).unwrap();
                    Some(User {
                        id: Utils::from_cass_uuid_to_uuid(host_id),
                        name: Result::ok( row.get(1) ).unwrap(),
                    })
                }
                else {
                    None
                }
            }
        }
    }

    pub async fn load_participants_room(&self, room_id: &str) -> Option<Vec<User>> {
        let mut statement = stmt!(Self::SELECT_PARTICIPANTS);
        statement.bind_string(0, room_id).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Err(_) => None,
            Ok(cass_result) => {
                if let Some(row) = cass_result.first_row() {
                    Utils::get_participants( Result::ok( row.get(0) ).unwrap() )
                }
                else {
                    None
                }
            }
        }
    }

    pub async fn user_exists(&self, user: User) -> bool {
        let mut statement = stmt!(Self::SELECT_EXISTS_QUERY);
        // statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid(user.id)).ok();
        statement.bind_string(0, user.name.as_str()).ok();

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

    pub async fn delete_user(&self, id: Uuid) -> Result<()> {
        let mut statement = stmt!(Self::DELETE_QUERY);
        let cass_uuid = cassandra_cpp::Uuid::from_str( id.to_string().as_str() ).ok().unwrap();
        statement.bind_uuid(0, cass_uuid).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = Result::ok(session.execute(&statement).wait());
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
                id: Utils::from_cass_uuid_to_uuid(user_id),
                name: Result::ok( row.get(1) ).unwrap(),
            }
        )
    }
}