CREATE TABLE IF NOT EXISTS chat_app.user (
    id UUID,
    name varchar,
    create_at bigint,
    PRIMARY KEY(id, name)
);