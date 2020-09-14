CREATE TABLE IF NOT EXISTS chat_app.room (
    room_id varchar PRIMARY KEY,
    room_title varchar,
    host_id UUID,
    host_name varchar,
    participants set<text>,
    scope varchar,
    create_at bigint,
    delete_key varchar,
);
