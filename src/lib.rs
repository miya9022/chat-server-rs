#[macro_use(lazy_static)]
extern crate lazy_static;

pub mod model;
pub mod proto;
pub mod hub;
pub mod server;
pub mod client;
pub mod error;
pub mod room_storage;

pub mod cass;
pub mod domain;