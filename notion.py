from __future__ import annotations

import os
import logging
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from notion_client import Client

logger = logging.getLogger(__name__)

notion = Client(auth=os.environ.get("NOTION_API_KEY"))
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")

TASKS_DB = os.environ.get("NOTION_TASKS_DB_ID", "346ff59abbff808baafcf114c2b618ac")
UPDATES_DB = os.environ.get("NOTION_UPDATES_DB_ID", "346ff59abbff8095b7b2c4b8698ea070")



def _query_database(db_id: str, filter_config: dict) -> list[dict]:
    """Query Notion database using REST API directly."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    payload = json.dumps({"filter": filter_config}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            return data.get("results", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logger.error(f"Database query failed: {e} | {body}")
        return []
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return []


def create_task(task: str, owner: str, notify: str, due_date: str, source: str, created_by: str, task_type: str = "To-do", group: str | None = None) -> dict:
    return notion.pages.create(
        parent={"database_id": TASKS_DB},
        properties={
            "Tasks": {"title": [{"text": {"content": task}}]},
            "Owner": {"select": {"name": owner}},
            "Notify": {"multi_select": [{"name": n} for n in _parse_notify(notify)]},
            "Due Date": {"date": {"start": due_date}},
            "Source": {"select": {"name": source}},
            "Status": {"select": {"name": "Pending"}},
            "Created By": {"select": {"name": created_by}},
            "Created At": {"date": {"start": _now_iso()}},
            "Pre-Reminder Sent": {"checkbox": False},
            "Type": {"select": {"name": task_type}},
            **( {"Group": {"select": {"name": group}}} if group else {} ),
        }
    )


def search_tasks(task_name: str | None, owner: str, statuses: list[str]) -> list[dict]:
    status_filters = [{"property": "Status", "select": {"equals": s}} for s in statuses]
    query_filter = {
        "and": [
            {"property": "Owner", "select": {"equals": owner}},
            {"or": status_filters}
        ]
    }
    results = _query_database(TASKS_DB, query_filter)
    if not task_name:
        return [_summarize(r) for r in results]

    name_lower = task_name.lower().strip()
    # Only words 4+ chars count — strips stopwords AND short noise words like "at", "for", "with"
    meaningful_words = [w for w in name_lower.split() if len(w) >= 4]

    scored = []
    for r in results:
        title = _get_title(r).lower()
        if title == name_lower:
            scored.append((0, r))
        elif name_lower in title or title in name_lower:
            scored.append((1, r))
        elif meaningful_words and all(w in title for w in meaningful_words):
            scored.append((2, r))
        # tier 4 (any-word match) removed — too broad, causes false positives

    scored.sort(key=lambda x: x[0])
    return [_summarize(r) for _, r in scored]


def delete_task(task_id: str) -> dict:
    return notion.pages.update(page_id=task_id, archived=True)


def mark_pre_reminder_sent(task_id: str) -> dict:
    return notion.pages.update(
        page_id=task_id,
        properties={"Pre-Reminder Sent": {"checkbox": True}}
    )


def get_upcoming_tasks(date_from: datetime, date_to: datetime) -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Due Date", "date": {"on_or_after": date_from.isoformat()}},
                {"property": "Due Date", "date": {"on_or_before": date_to.isoformat()}},
                {"property": "Status", "select": {"equals": "Pending"}},
                {"property": "Pre-Reminder Sent", "checkbox": {"equals": False}},
                {"property": "Type", "select": {"equals": "Event"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


def check_event_conflicts(dt_iso: str, persons: list[str], window_minutes: int = 60) -> list[dict]:
    """Return Events within window_minutes of dt_iso for any of the given persons."""
    from datetime import timedelta
    dt = datetime.fromisoformat(dt_iso).astimezone(timezone.utc)
    window_start = (dt - timedelta(minutes=window_minutes)).isoformat()
    window_end = (dt + timedelta(minutes=window_minutes)).isoformat()

    owner_part = (
        {"or": [{"property": "Owner", "select": {"equals": p}} for p in persons]}
        if len(persons) > 1
        else {"property": "Owner", "select": {"equals": persons[0]}}
    )
    results = _query_database(TASKS_DB, {
        "and": [
            {"property": "Due Date", "date": {"on_or_after": window_start}},
            {"property": "Due Date", "date": {"on_or_before": window_end}},
            {"property": "Type", "select": {"equals": "Event"}},
            {"property": "Status", "select": {"does_not_equal": "Done"}},
            owner_part,
        ]
    })
    return [_summarize(r) for r in results]


def get_tasks_by_group(group_name: str) -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Group", "select": {"equals": group_name}},
                {"property": "Status", "select": {"does_not_equal": "Done"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


_ACTIVE_TODO_STATUSES = ["Pending", "In Progress", "Awaiting Confirmation"]
_ACTIVE_STATUS_FILTER = {"or": [{"property": "Status", "select": {"equals": s}} for s in _ACTIVE_TODO_STATUSES]}


def get_pending_todos(date_from: datetime, date_to: datetime) -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Due Date", "date": {"on_or_after": date_from.isoformat()}},
                {"property": "Due Date", "date": {"on_or_before": date_to.isoformat()}},
                _ACTIVE_STATUS_FILTER,
                {"property": "Type", "select": {"equals": "To-do"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


def get_overdue_todos() -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Due Date", "date": {"before": _now_iso()}},
                _ACTIVE_STATUS_FILTER,
                {"property": "Type", "select": {"equals": "To-do"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


def update_task_done(task_id: str) -> dict:
    return notion.pages.update(
        page_id=task_id,
        properties={
            "Status": {"select": {"name": "Done"}},
            "Completed At": {"date": {"start": _now_iso()}},
        }
    )


def update_task_status(task_id: str, status: str) -> dict:
    return notion.pages.update(
        page_id=task_id,
        properties={"Status": {"select": {"name": status}}}
    )


def update_task_fields(task_id: str, fields: dict) -> dict:
    properties = {}
    if "due_date" in fields:
        properties["Due Date"] = {"date": {"start": fields["due_date"]}}
    if "owner" in fields:
        properties["Owner"] = {"select": {"name": fields["owner"]}}
    if "notes" in fields:
        properties["Notes"] = {"rich_text": [{"text": {"content": fields["notes"]}}]}
    if "notify" in fields:
        properties["Notify"] = {"multi_select": [{"name": n} for n in _parse_notify(fields["notify"])]}
    if "group" in fields:
        properties["Group"] = {"select": {"name": fields["group"]}} if fields["group"] else {"select": None}
    return notion.pages.update(page_id=task_id, properties=properties)


def get_task_status(task_name: str | None, owner: str | None) -> dict | None:
    if owner:
        results = search_tasks(task_name, owner, ["Pending", "In Progress", "Snoozed", "Awaiting Confirmation"])
    else:
        q = {"property": "Tasks", "title": {"contains": task_name}} if task_name else {}
        raw = _query_database(TASKS_DB, q)
        results = [_summarize(r) for r in raw]
    return results[0] if results else None


def get_tasks_due(date_from: datetime, date_to: datetime) -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Due Date", "date": {"on_or_after": date_from.isoformat()}},
                {"property": "Due Date", "date": {"on_or_before": date_to.isoformat()}},
                {"property": "Status", "select": {"does_not_equal": "Done"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


def get_overdue_tasks() -> list[dict]:
    results = _query_database(
        TASKS_DB,
        {
            "and": [
                {"property": "Due Date", "date": {"before": _now_iso()}},
                {"property": "Status", "select": {"does_not_equal": "Done"}},
                {"property": "Status", "select": {"does_not_equal": "Snoozed"}},
            ]
        }
    )
    return [_summarize(r) for r in results]


def log_update_request(task_id: str, requested_by: str) -> dict:
    return notion.pages.create(
        parent={"database_id": UPDATES_DB},
        properties={
            "Update": {"title": [{"text": {"content": f"Status update requested by {requested_by}"}}]},
            "Linked Task": {"relation": [{"id": task_id}]},
            "Requested By": {"select": {"name": requested_by}},
            "Requested At": {"date": {"start": _now_iso()}},
        }
    )


# ── Pending action persistence (Bug 1) ───────────────────────────────────────
# Requires "Pending Action" rich_text property in Notion Tasks DB.

def set_pending_action(task_id: str, action_json: str) -> None:
    """Write pending action JSON and set status to Awaiting Confirmation so the fallback query can find it after a restart."""
    try:
        notion.pages.update(
            page_id=task_id,
            properties={
                "Pending Action": {"rich_text": [{"text": {"content": action_json[:2000]}}]},
                "Status": {"select": {"name": "Awaiting Confirmation"}},
            }
        )
    except Exception as e:
        logger.warning(f"set_pending_action failed (add 'Pending Action' rich_text to Notion Tasks DB): {e}")


def clear_pending_action(task_id: str) -> None:
    try:
        notion.pages.update(
            page_id=task_id,
            properties={"Pending Action": {"rich_text": []}}
        )
    except Exception as e:
        logger.warning(f"clear_pending_action failed: {e}")


def get_pending_action_for_user(owner: str) -> dict | None:
    results = _query_database(TASKS_DB, {
        "and": [
            {"property": "Owner", "select": {"equals": owner}},
            {"property": "Status", "select": {"equals": "Awaiting Confirmation"}},
        ]
    })
    if not results:
        return None
    page = results[0]
    try:
        raw = page["properties"].get("Pending Action", {}).get("rich_text", [])
        if not raw:
            return None
        data = json.loads(raw[0]["text"]["content"])
        data["task_id"] = page["id"]
        data["task_name"] = _get_title(page)
        return data
    except Exception as e:
        logger.warning(f"get_pending_action_for_user parse failed: {e}")
        return None


# ── Draft task persistence (Bug 5) ───────────────────────────────────────────
# Requires "Draft Data" rich_text property and "Draft" Status option in Notion Tasks DB.

def create_draft_task(partial_data: dict, created_by: str) -> dict:
    task_name = partial_data.get("task") or "(Draft)"
    owner = partial_data.get("owner") or created_by
    props = {
        "Tasks": {"title": [{"text": {"content": task_name}}]},
        "Owner": {"select": {"name": owner}},
        "Status": {"select": {"name": "Draft"}},
        "Created By": {"select": {"name": created_by}},
        "Created At": {"date": {"start": _now_iso()}},
        "Type": {"select": {"name": partial_data.get("type") or "To-do"}},
        "Pre-Reminder Sent": {"checkbox": False},
        "Draft Data": {"rich_text": [{"text": {"content": json.dumps(partial_data)[:2000]}}]},
    }
    if partial_data.get("notify"):
        props["Notify"] = {"multi_select": [{"name": n} for n in _parse_notify(partial_data["notify"])]}
    if partial_data.get("due_date"):
        props["Due Date"] = {"date": {"start": partial_data["due_date"]}}
    return notion.pages.create(parent={"database_id": TASKS_DB}, properties=props)


def get_draft_for_user(created_by: str) -> dict | None:
    results = _query_database(TASKS_DB, {
        "and": [
            {"property": "Status", "select": {"equals": "Draft"}},
            {"property": "Created By", "select": {"equals": created_by}},
        ]
    })
    if not results:
        return None
    page = results[0]
    try:
        raw = page["properties"].get("Draft Data", {}).get("rich_text", [])
        if not raw:
            return None
        data = json.loads(raw[0]["text"]["content"])
        data["draft_task_id"] = page["id"]
        return data
    except Exception as e:
        logger.warning(f"get_draft_for_user parse failed: {e}")
        return None


def clear_draft(task_id: str) -> None:
    try:
        notion.pages.update(page_id=task_id, archived=True)
    except Exception as e:
        logger.warning(f"clear_draft failed: {e}")


# ── helpers ──────────────────────────────────────────────────────────────────

def _summarize(page: dict) -> dict:
    props = page["properties"]
    notify_items = props.get("Notify", {}).get("multi_select", [])
    notify_names = [n["name"] for n in notify_items]
    notify = "Both" if len(notify_names) > 1 else (notify_names[0] if notify_names else "")
    return {
        "id": page["id"],
        "task": _get_title(page),
        "owner": _get_select(props, "Owner"),
        "status": _get_select(props, "Status"),
        "due_date": _get_date(props, "Due Date"),
        "notify": notify,
        "notes": _get_text(props, "Notes"),
        "group": _get_select(props, "Group"),
        "url": page.get("url", ""),
    }


def _parse_notify(notify: str) -> list[str]:
    return ["Jaime", "Denise"] if notify == "Both" else [notify]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_title(page: dict) -> str:
    try:
        return page["properties"]["Tasks"]["title"][0]["text"]["content"]
    except (KeyError, IndexError):
        return ""


def _get_select(props: dict, field: str) -> str:
    try:
        return props[field]["select"]["name"]
    except (KeyError, TypeError):
        return ""


def _get_text(props: dict, field: str) -> str:
    try:
        return props[field]["rich_text"][0]["text"]["content"]
    except (KeyError, IndexError):
        return ""


def _get_date(props: dict, field: str) -> str:
    try:
        return props[field]["date"]["start"]
    except (KeyError, TypeError):
        return ""
