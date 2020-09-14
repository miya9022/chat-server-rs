CREATE TABLE IF NOT EXISTS chat_app.message (
    id TIMEUUID,
    from_id UUID,
    from_name VARCHAR,
    room_id VARCHAR,
    body text,
    PRIMARY KEY ((from_id, room_id), id )
)
WITH CLUSTERING ORDER BY (id DESC);