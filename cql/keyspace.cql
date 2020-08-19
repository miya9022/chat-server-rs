CREATE KEYSPACE IF NOT EXISTS chat_app
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor' : 1
    };