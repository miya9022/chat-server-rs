CREATE TABLE IF NOT EXISTS chat_app.message (
    id UUID,
    user_id UUID,
    user_name varchar,
    room_id varchar,
    body text,
    create_at bigint,
    PRIMARY KEY ( id, create_at )
)
WITH CLUSTERING ORDER BY (create_at ASC);