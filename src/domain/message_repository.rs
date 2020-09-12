use async_trait::async_trait;
use cassandra_cpp::*;
use uuid::Uuid;
use tokio::sync::Mutex;

use crate::model::user::User;
use crate::model::message::Message;
use crate::domain::repository::{Repository, Utils};

pub struct MessageRepository {
    pub(crate) cluster: Mutex<Cluster>
}

#[async_trait]
impl Repository for MessageRepository {

    async fn retrieve_session(&self) -> Result<Session> {
        let mut cluster_ = self.cluster.lock().await;
        cluster_.connect_async().await
    }
}

impl MessageRepository {
    const INSERT_QUERY: &'static str = "INSERT INTO chat_app.message \
    (id, from_id, from_name, to_id, to_name, room_id, body) \
    VALUES \
    (now(), ?, ?, ?, ?, ?, ?)";

    const UPDATE_BODY_QUERY: &'static str = "UPDATE chat_app.message SET body = ? WHERE \
      id = ? AND \
      from_id = ? AND \
      to_id = ?";

    const SELECT_ALL_BY_ROOM_ID_QUERY: &'static str = "\
    SELECT id, from_id, from_name, to_id, to_name, room_id, body \
    FROM chat_app.message \
    WHERE from_id = ? \
      AND to_id = ? \
      AND room_id = ? \
    ALLOW FILTERING";

    const SELECT_ONE_QUERY: &'static str = "\
    SELECT id, from_id, from_name, to_id, to_name, room_id, body \
    FROM chat_app.message \
    WHERE id = ? ALLOW FILTERING";

    const SELECT_EXIST_QUERY: &'static str = "SELECT COUNT(*) FROM chat_app.message WHERE id = ? ALLOW FILTERING";

    const DELETE_QUERY: &'static str = "DELETE FROM chat_app.message WHERE \
      id = ? AND \
      from_id = ? AND \
      to_id = ?";

    pub async fn add_new_message(&self, room_id: &str, message: Message) -> Option<Message> {
        let persistent_msg = message.clone();
        let mut statement = stmt!(Self::INSERT_QUERY);
        statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid(persistent_msg.from.id)).ok();
        statement.bind_string(1, persistent_msg.from.name.as_str()).ok();
        statement.bind_uuid(2, Utils::from_uuid_to_cass_uuid(persistent_msg.to.id)).ok();
        statement.bind_string(3, persistent_msg.to.name.as_str()).ok();
        statement.bind_string(4, room_id).ok();
        statement.bind_string(5, persistent_msg.body.as_str()).ok();

        let session = self.retrieve_session().await.unwrap();
        let result = session.execute(&statement).wait();
        match result {
            Ok(_) => Some(message),
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn update_message_body(&self, msg_id: Uuid, from_id: Uuid, to_id: Uuid, body: &str) -> Option<Message> {
        if !self.check_message_exists(msg_id).await {
            println!("message doesn't exists");
            return None;
        }

        let mut statement = stmt!(Self::UPDATE_BODY_QUERY);
        statement.bind_string(0, body).ok();
        statement.bind_uuid(1, Utils::from_uuid_to_cass_uuid(msg_id)).ok();
        statement.bind_uuid(2, Utils::from_uuid_to_cass_uuid(from_id)).ok();
        statement.bind_uuid(3, Utils::from_uuid_to_cass_uuid(to_id)).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Ok(_) => self.load_one_message(msg_id).await,
            Err(error) => {
                println!("Something bad happen: {:?}", error);
                None
            }
        }
    }

    pub async fn load_messages_by_room(&self, room_id: &str, from_id: Uuid, to_id: Uuid, page: i32, size: i32) -> Option<Vec<Message>> {
        let mut res = Vec::<Message>::new();

        let mut statement = stmt!(Self::SELECT_ALL_BY_ROOM_ID_QUERY);
        statement.bind_uuid(0, Utils::from_uuid_to_cass_uuid(from_id)).ok();
        statement.bind_uuid(1, Utils::from_uuid_to_cass_uuid(to_id)).ok();
        statement.bind_string(2, room_id).ok();
        statement.set_paging_size(size).ok();
        let mut has_more_pages = true;
        let mut paging = page - 1;

        let session = self.retrieve_session().await.unwrap();
        while has_more_pages && paging >= 0 {
            match session.execute(&statement).wait() {
                Err(_) => break,
                Ok(result) => {

                    if paging == 0 {
                        for row in result.iter() {
                            res.push(Self::bind_to_message(row).unwrap() );
                        }
                    }

                    has_more_pages = result.has_more_pages();
                    if has_more_pages {
                        statement.set_paging_state(result).ok();
                    }
                    paging -= 1;
                }
            }
        }

        Some(res)
    }

    pub async fn load_one_message(&self, msg_id: Uuid) -> Option<Message> {
        let msg_id = Utils::from_uuid_to_cass_uuid(msg_id);
        let mut statement = stmt!(Self::SELECT_ONE_QUERY);
        statement.bind_uuid(0,msg_id).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Ok(result) => {
                if let Some(row) = result.first_row() {
                    Self::bind_to_message(row)
                }
                else {
                    None
                }
            }
            Err(_) => {
                println!("message not found");
                None
            }
        }
    }

    pub async fn check_message_exists(&self, msg_id: Uuid) -> bool {
        let msg_id = Utils::from_uuid_to_cass_uuid(msg_id);
        let mut statement = stmt!(Self::SELECT_EXIST_QUERY);
        statement.bind_uuid(0,msg_id).ok();
        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Ok(res) => {
                res
                    .first_row().unwrap()
                    .get_column(0).unwrap()
                    .get_i64().unwrap()
                    > 0
            }
            Err(_) => false
        }
    }

    pub async fn delete_message(&self, msg_id: Uuid, from_id: Uuid, to_id: Uuid) -> Result<()> {
        let msg_id = Utils::from_uuid_to_cass_uuid(msg_id);
        let mut statement = stmt!(Self::DELETE_QUERY);
        statement.bind_uuid(0, msg_id).ok();
        statement.bind_uuid(1, Utils::from_uuid_to_cass_uuid(from_id)).ok();
        statement.bind_uuid(2, Utils::from_uuid_to_cass_uuid(to_id)).ok();

        let session = self.retrieve_session().await.unwrap();
        match session.execute(&statement).wait() {
            Ok(_) => Ok(()),
            Err(_) => {
                Err(Error::from_kind(ErrorKind::Msg("Delete user failed".to_string())))
            }
        }
    }

    fn bind_to_message(row: Row) -> Option<Message> {
        let msg_id: cassandra_cpp::Uuid = Result::ok( row.get(0) ).unwrap();
        let from_id: cassandra_cpp::Uuid = Result::ok( row.get(1) ).unwrap();
        let to_id: cassandra_cpp::Uuid = Result::ok( row.get(3) ).unwrap();
        Some(
            Message {
                id: Utils::from_cass_uuid_to_uuid(msg_id),
                from: User {
                    id: Utils::from_cass_uuid_to_uuid(from_id),
                    name: Result::ok( row.get(2) ).unwrap(),
                },
                to: User {
                    id: Utils::from_cass_uuid_to_uuid(to_id),
                    name: Result::ok( row.get(4) ).unwrap(),
                },
                room_id: Result::ok( row.get(5) ).unwrap(),
                body: Result::ok( row.get(6) ).unwrap(),
            }
        )
    }
}