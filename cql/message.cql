CREATE TABLE IF NOT EXISTS chat_app.message (
    id TIMEUUID,

    from_id UUID,
    from_name VARCHAR,

    to_id UUID,
    to_name VARCHAR,

    room_id VARCHAR,
    body text,
    PRIMARY KEY ((from_id, to_id), id )
)
WITH CLUSTERING ORDER BY (id DESC);