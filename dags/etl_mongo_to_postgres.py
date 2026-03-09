from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2


MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB = "etl_project_db"

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def load_user_sessions():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    collection = mongo_db["user_sessions"]

    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()

    docs = list(collection.find())

    for doc in docs:
        session_id = doc["session_id"]
        user_id = doc["user_id"]
        start_time = doc["start_time"]
        end_time = doc["end_time"]
        pages_visited = doc.get("pages_visited", [])
        device = doc.get("device")
        actions = doc.get("actions", [])

        session_duration_minutes = round(
            (end_time - start_time).total_seconds() / 60, 2
        )
        pages_count = len(pages_visited)
        actions_count = len(actions)

        pg_cur.execute(
            """
            INSERT INTO stg_user_sessions (
                session_id, user_id, start_time, end_time,
                session_duration_minutes, pages_visited, pages_count,
                device, actions, actions_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                session_duration_minutes = EXCLUDED.session_duration_minutes,
                pages_visited = EXCLUDED.pages_visited,
                pages_count = EXCLUDED.pages_count,
                device = EXCLUDED.device,
                actions = EXCLUDED.actions,
                actions_count = EXCLUDED.actions_count
        """,
            (
                session_id,
                user_id,
                start_time,
                end_time,
                session_duration_minutes,
                pages_visited,
                pages_count,
                device,
                actions,
                actions_count,
            ),
        )

    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    mongo_client.close()


def load_event_logs():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    collection = mongo_db["event_logs"]

    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()

    docs = list(collection.find())

    for doc in docs:
        event_id = doc["event_id"]
        session_id = doc.get("session_id")
        user_id = doc.get("user_id")
        event_timestamp = doc["timestamp"]
        event_type = doc["event_type"]
        details = str(doc.get("details", ""))

        pg_cur.execute(
            """
            INSERT INTO stg_event_logs (
                event_id, session_id, user_id, event_timestamp, event_type, details
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                session_id = EXCLUDED.session_id,
                user_id = EXCLUDED.user_id,
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                details = EXCLUDED.details
        """,
            (event_id, session_id, user_id, event_timestamp, event_type, details),
        )

    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    mongo_client.close()


def load_support_tickets():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    collection = mongo_db["support_tickets"]

    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()

    docs = list(collection.find())

    for doc in docs:
        ticket_id = doc["ticket_id"]
        user_id = doc["user_id"]
        status = doc["status"]
        issue_type = doc["issue_type"]
        created_at = doc["created_at"]
        updated_at = doc["updated_at"]
        messages = doc.get("messages", [])

        resolution_minutes = round((updated_at - created_at).total_seconds() / 60, 2)
        messages_count = len(messages)

        pg_cur.execute(
            """
            INSERT INTO stg_support_tickets (
                ticket_id, user_id, status, issue_type,
                created_at, updated_at, resolution_minutes, messages_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                status = EXCLUDED.status,
                issue_type = EXCLUDED.issue_type,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                resolution_minutes = EXCLUDED.resolution_minutes,
                messages_count = EXCLUDED.messages_count
        """,
            (
                ticket_id,
                user_id,
                status,
                issue_type,
                created_at,
                updated_at,
                resolution_minutes,
                messages_count,
            ),
        )

    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    mongo_client.close()


default_args = {"owner": "airflow"}

with DAG(
    dag_id="etl_mongo_to_postgres",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "mongo", "postgres"],
) as dag:

    load_sessions_task = PythonOperator(
        task_id="load_user_sessions", python_callable=load_user_sessions
    )

    load_events_task = PythonOperator(
        task_id="load_event_logs", python_callable=load_event_logs
    )

    load_tickets_task = PythonOperator(
        task_id="load_support_tickets", python_callable=load_support_tickets
    )

    [load_sessions_task, load_events_task, load_tickets_task]
