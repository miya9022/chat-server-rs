use cassandra_cpp::*;
use std::fs;
use std::sync::Arc;

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
            session: unsafe { Arc::from_raw(session) }
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
            Ok(stmt) => Ok(stmt),
            _ => Err(Error::from_kind(ErrorKind::Msg("unable to read the file".to_string()))),
        };

        match schema_stmt.as_ref().map(|stmt| stmt!(stmt)) {
            Ok(stmt) => {
                self.session.execute(&stmt).await
            }
            Err(error) => {
                Err(Error::from_kind(ErrorKind::Msg(error.to_string())))
            }
        }
    }
}