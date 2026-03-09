import random
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017/")
db = client["etl_project_db"]

user_sessions_col = db["user_sessions"]
event_logs_col = db["event_logs"]
support_tickets_col = db["support_tickets"]

user_sessions_col.delete_many({})
event_logs_col.delete_many({})
support_tickets_col.delete_many({})

random.seed(42)

NUM_USERS = 100
NUM_SESSIONS = 1000
NUM_TICKETS = 300

users = [f"user_{i:03d}" for i in range(1, NUM_USERS + 1)]
devices = ["mobile", "desktop", "tablet"]
pages = ["/home", "/catalog", "/search", "/cart", "/checkout", "/profile"]
product_pages = [f"/product/{i}" for i in range(1, 51)]
event_types = ["click", "view", "scroll", "add_to_cart", "purchase", "login", "logout"]
actions_pool = ["login", "view_product", "search", "add_to_cart", "checkout", "logout"]
ticket_statuses = ["open", "in_progress", "closed"]
issue_types = ["payment", "delivery", "refund", "account", "technical"]

start_base = datetime(2026, 2, 1, tzinfo=timezone.utc)

sessions = []
events = []
tickets = []

for i in range(1, NUM_SESSIONS + 1):
    session_id = f"sess_{i:06d}"
    user_id = random.choice(users)

    session_start = start_base + timedelta(
        days=random.randint(0, 27),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    duration = random.randint(5, 90)
    session_end = session_start + timedelta(minutes=duration)

    visited = random.sample(pages + product_pages, random.randint(2, 6))
    actions = random.sample(actions_pool, random.randint(2, 5))
    device = random.choice(devices)

    session_doc = {
        "session_id": session_id,
        "user_id": user_id,
        "start_time": session_start,
        "end_time": session_end,
        "pages_visited": visited,
        "device": device,
        "actions": actions,
    }
    sessions.append(session_doc)

    num_events = random.randint(3, 8)
    for j in range(num_events):
        event_time = session_start + timedelta(minutes=random.randint(0, duration))
        event_doc = {
            "event_id": f"evt_{i:06d}_{j+1}",
            "session_id": session_id,
            "user_id": user_id,
            "timestamp": event_time,
            "event_type": random.choice(event_types),
            "details": random.choice(visited),
        }
        events.append(event_doc)

for i in range(1, NUM_TICKETS + 1):
    ticket_id = f"ticket_{i:06d}"
    user_id = random.choice(users)

    created_at = start_base + timedelta(
        days=random.randint(0, 27),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    status = random.choice(ticket_statuses)
    issue_type = random.choice(issue_types)

    if status == "open":
        updated_at = created_at + timedelta(minutes=random.randint(10, 180))
    elif status == "in_progress":
        updated_at = created_at + timedelta(hours=random.randint(1, 24))
    else:
        updated_at = created_at + timedelta(hours=random.randint(2, 72))

    msg_count = random.randint(2, 5)
    messages = []
    for j in range(msg_count):
        sender = "user" if j % 2 == 0 else "support"
        msg_time = created_at + timedelta(minutes=10 * j)
        messages.append(
            {
                "sender": sender,
                "message": f"Сообщение {j+1} по тикету {ticket_id}",
                "timestamp": msg_time,
            }
        )

    ticket_doc = {
        "ticket_id": ticket_id,
        "user_id": user_id,
        "status": status,
        "issue_type": issue_type,
        "messages": messages,
        "created_at": created_at,
        "updated_at": updated_at,
    }
    tickets.append(ticket_doc)

user_sessions_col.insert_many(sessions)
event_logs_col.insert_many(events)
support_tickets_col.insert_many(tickets)

print(f"user_sessions: {len(sessions)}")
print(f"event_logs: {len(events)}")
print(f"support_tickets: {len(tickets)}")
