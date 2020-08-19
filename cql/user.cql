CREATE TABLE IF NOT EXISTS chat_app.user (
    id UUID PRIMARY KEY,
    name varchar,
    create_at bigint,
);