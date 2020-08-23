use crate::model::message::Message;

#[derive(Default)]
pub struct Feed {
  messages: Vec<Message>,
  size: usize,
}

impl Feed {
  pub fn add_message(&mut self, message: Message) {
    self.messages.push(message);
    self.messages.sort_by_key(|message| message.created_at);

    self.size += 1;
  }

  pub fn messages_iter(&self) -> impl Iterator<Item = &Message> {
    self.messages.iter()
  }

  pub fn is_empty(&self) -> bool {
    self.size <= 0
  }
}