CREATE TABLE IF NOT EXISTS chat_app.message (
    id UUID PRIMARY KEY,
    user_id UUID,
    user_name varchar,
    room_id varchar,
    body text,
    create_at bigint
);