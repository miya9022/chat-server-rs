CREATE TABLE IF NOT EXISTS chat_app.room_users (
    room_id varchar,
    user_id UUID,
    room_title varchar,
    create_at bigint,
    PRIMARY KEY (room_id, user_id)
);