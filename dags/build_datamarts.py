from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def build_user_activity_mart():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE mart_user_activity;")

    cur.execute(
        """
        INSERT INTO mart_user_activity (
            activity_date,
            user_id,
            sessions_count,
            avg_session_duration_minutes,
            total_pages_visited,
            total_actions
        )
        SELECT
            DATE(start_time) AS activity_date,
            user_id,
            COUNT(*) AS sessions_count,
            ROUND(AVG(session_duration_minutes), 2) AS avg_session_duration_minutes,
            SUM(pages_count) AS total_pages_visited,
            SUM(actions_count) AS total_actions
        FROM stg_user_sessions
        GROUP BY DATE(start_time), user_id
        ORDER BY activity_date, user_id;
    """
    )

    conn.commit()
    cur.close()
    conn.close()


def build_support_performance_mart():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE mart_support_performance;")

    cur.execute(
        """
        INSERT INTO mart_support_performance (
            report_date,
            issue_type,
            status,
            tickets_count,
            avg_resolution_minutes,
            open_tickets_count
        )
        SELECT
            DATE(created_at) AS report_date,
            issue_type,
            status,
            COUNT(*) AS tickets_count,
            ROUND(AVG(resolution_minutes), 2) AS avg_resolution_minutes,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_tickets_count
        FROM stg_support_tickets
        GROUP BY DATE(created_at), issue_type, status
        ORDER BY report_date, issue_type, status;
    """
    )

    conn.commit()
    cur.close()
    conn.close()


default_args = {"owner": "airflow"}

with DAG(
    dag_id="build_datamarts",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=["datamart", "analytics", "postgres"],
) as dag:

    build_user_activity = PythonOperator(
        task_id="build_user_activity_mart", python_callable=build_user_activity_mart
    )

    build_support_performance = PythonOperator(
        task_id="build_support_performance_mart",
        python_callable=build_support_performance_mart,
    )

    [build_user_activity, build_support_performance]
