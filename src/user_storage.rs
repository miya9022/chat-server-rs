use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use futures::StreamExt;
use uuid::Uuid;

use crate::proto::{OutputParcel, InputParcel, Input, RoomOutput, Output, RoomsLoadedOutput};
use crate::domain::room_user_repository::RoomUserRepository;
use crate::domain::repository::RepositoryFactory;
use crate::utils::AppUtils;

const OUTPUT_CHANNEL_SIZE: usize = 16;

pub struct UserStorage {
    output_sender: broadcast::Sender<OutputParcel>,
    room_user_repo: Arc<RoomUserRepository>
}

impl UserStorage {
    pub fn new(repo_fact: &RepositoryFactory) -> Self {
        let (output_sender, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);

        let room_user_repo = match AppUtils::downcast_arc::<RoomUserRepository>(
            repo_fact.get_repository("ROOM_USERS")) {
            Ok(repo) => repo,
            Err(_) => panic!("can't find repository")
        };

        UserStorage {
            output_sender,
            room_user_repo,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OutputParcel> {
        self.output_sender.subscribe()
    }

    pub async fn run(&self, receiver: UnboundedReceiver<InputParcel>) {
        let processing = receiver.for_each(|input_parcel| self.process(input_parcel));

        tokio::select! {
          _ = processing => {},
        }
    }

    async fn process(&self, input_parcel: InputParcel) {
        match input_parcel.input {
            Input::Ping => self.send_pong(input_parcel.client_id),
            Input::LoadRooms(input) => self.load_rooms(input.user_id).await,
            _ => unimplemented!()
        }
    }

    fn send_pong(&self, user_id: Uuid) {
        self.output_sender
            .send(OutputParcel::new("".to_string(), user_id, Output::Pong))
            .unwrap();
    }

    async fn load_rooms(&self, user_id: Uuid) {
        let result = self.room_user_repo.load_by_userid(user_id, 1, 10).await
                    .as_ref()
                    .map(|rooms|
                        rooms
                            .iter()
                            .map(|room| {
                                RoomOutput::new(
                                    room.room_id.clone(),
                                    room.room_title.clone(),
                                    room.user_id,
                                    room.create_at
                                )
                    })
                    .collect());

        self.output_sender
            .send(
                OutputParcel::new(
                    "".to_string(),
                    user_id,
                    Output::RoomsLoaded(RoomsLoadedOutput::new(result.unwrap())))
            )
            .unwrap();
    }
}