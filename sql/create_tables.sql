CREATE TABLE IF NOT EXISTS stg_user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    session_duration_minutes NUMERIC(10,2),
    pages_visited TEXT[],
    pages_count INT,
    device TEXT,
    actions TEXT[],
    actions_count INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_event_logs (
    event_id TEXT PRIMARY KEY,
    session_id TEXT,
    user_id TEXT,
    event_timestamp TIMESTAMP NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    resolution_minutes NUMERIC(10,2),
    messages_count INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mart_user_activity (
    activity_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    sessions_count INT,
    avg_session_duration_minutes NUMERIC(10,2),
    total_pages_visited INT,
    total_actions INT
);

CREATE TABLE IF NOT EXISTS mart_support_performance (
    report_date DATE NOT NULL,
    issue_type TEXT NOT NULL,
    status TEXT NOT NULL,
    tickets_count INT,
    avg_resolution_minutes NUMERIC(10,2),
    open_tickets_count INT
);
