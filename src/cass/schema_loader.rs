use cassandra_cpp::*;
use std::fs;
use std::sync::Arc;
use std::borrow::BorrowMut;

#[derive(Debug, Default)]
pub struct SchemaLoader {
    session: Arc<Session>,
}

impl SchemaLoader {

    pub fn new() -> Self {
        SchemaLoader {
            session: Arc::new(Session::default())
        }
    }

    pub fn new_with_session(session: &mut Session) -> Self {
        SchemaLoader {
            session: unsafe {
                Arc::from_raw(session)
            }
        }
    }

    pub fn set_session(&mut self, session: Session) {
        self.session = Arc::new(session);
    }

    pub fn get_session(&self) -> &Session {
        &self.session
    }

    pub async fn load_from_file(&self, file_path: String) -> Result<CassResult> {
        let schema_stmt = match fs::read_to_string(file_path.as_str()) {
            Ok(stmt) => Some(stmt),
            _ => None,
        };
        let schema_stmt = schema_stmt.as_ref().map(|stmt| stmt!(stmt));

        if let Some(stmt) = schema_stmt {
            self.session.execute(&stmt).await
        }
        else {
            Err(Error::from_kind(ErrorKind::Msg("wrong kind".to_string())))
        }
    }
}