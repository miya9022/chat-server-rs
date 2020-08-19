CREATE TABLE IF NOT EXISTS chat_app.room (
    room_id varchar PRIMARY KEY,
    host_id UUID,
    host_name varchar,
    participants set<varchar>,
    create_at bigint,
    delete_key varchar,
)