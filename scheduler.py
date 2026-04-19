from __future__ import annotations

import html
import os
import logging
import datetime
from datetime import timedelta, timezone
import pytz
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from handlers import pending_confirmations

logger = logging.getLogger(__name__)

MANILA_TZ = pytz.timezone("Asia/Manila")
GROUP_ID = int(os.environ.get("TELEGRAM_GROUP_ID", -1003856456479))
JAIME_ID = int(os.environ.get("TELEGRAM_JAIME_ID", 110907401))
DENISE_ID = int(os.environ.get("TELEGRAM_DENISE_ID", 219845668))
TASKS_DB_ID = os.environ.get("NOTION_TASKS_DB_ID", "346ff59abbff808baafcf114c2b618ac")

_DIVIDER = "───────────────"


def _esc(s) -> str:
    return html.escape(str(s)) if s else ""


def setup_scheduler(app) -> None:
    digest_time = datetime.time(8, 0, 0, tzinfo=MANILA_TZ)
    nudge_time = datetime.time(9, 0, 0, tzinfo=MANILA_TZ)
    app.job_queue.run_repeating(check_reminders, interval=300, first=15)
    app.job_queue.run_daily(send_daily_digest, time=digest_time)
    app.job_queue.run_daily(send_todo_nudges, time=nudge_time)
    logger.info("Scheduler ready — poller every 5 min, digest 08:00, to-do nudges 09:00 Manila")


async def check_reminders(context: ContextTypes.DEFAULT_TYPE) -> None:
    import notion
    now = datetime.datetime.now(timezone.utc)
    window_start = now - timedelta(hours=1)

    # 30-minute advance notices
    try:
        upcoming = notion.get_upcoming_tasks(now, now + timedelta(minutes=30))
        for task in upcoming:
            owner = task["owner"]
            owner_id = JAIME_ID if owner == "Jaime" else DENISE_ID
            try:
                await context.bot.send_message(
                    chat_id=owner_id,
                    text=f"Heads up — <b>{_esc(task['task'])}</b> is due in about 30 minutes.",
                    parse_mode=ParseMode.HTML
                )
                notion.mark_pre_reminder_sent(task["id"])
                logger.info(f"Pre-reminder sent: '{task['task']}' for {owner}")
            except Exception as e:
                logger.error(f"Pre-reminder failed for '{task['task']}': {e}")
    except Exception as e:
        logger.error(f"check_reminders pre-reminder query failed: {e}")

    try:
        tasks = notion.get_tasks_due(window_start, now)
    except Exception as e:
        logger.error(f"check_reminders query failed: {e}")
        return

    for task in tasks:
        if task["status"] != "Pending":
            continue

        owner = task["owner"]
        owner_id = JAIME_ID if owner == "Jaime" else DENISE_ID

        try:
            await context.bot.send_message(
                chat_id=owner_id,
                text=f"Reminder: <b>{_esc(task['task'])}</b>. Reply 'done' once it's handled.",
                parse_mode=ParseMode.HTML
            )
            pending_confirmations[owner_id] = {
                "action": "mark_done",
                "task_id": task["id"],
                "task_name": task["task"],
                "owner": owner,
            }
        except Exception as e:
            logger.error(f"Failed to notify owner for '{task['task']}': {e}")
            continue

        if task.get("notify") == "Both":
            other_id = DENISE_ID if owner == "Jaime" else JAIME_ID
            try:
                await context.bot.send_message(
                    chat_id=other_id,
                    text=f"For your awareness: {_esc(owner)} has a reminder — <b>{_esc(task['task'])}</b>.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.warning(f"Could not notify other user ({other_id}) for '{task['task']}': {e}")

        try:
            notion.update_task_status(task["id"], "In Progress")
            logger.info(f"Fired reminder: '{task['task']}' for {owner}")
        except Exception as e:
            logger.error(f"Failed to update status for '{task['task']}': {e}")


def _fmt_task_date(due_date_str: str, now_manila: datetime.datetime) -> str:
    if not due_date_str:
        return ""
    try:
        dt = datetime.datetime.fromisoformat(due_date_str)
        if dt.tzinfo is None:
            dt = MANILA_TZ.localize(dt)
        else:
            dt = dt.astimezone(MANILA_TZ)
        today = now_manila.date()
        tomorrow = today + timedelta(days=1)
        has_time = dt.hour != 0 or dt.minute != 0
        time_str = f" {dt.strftime('%-I:%M %p')}" if has_time else ""
        if dt.date() in (today, tomorrow):
            return time_str.strip() if time_str else ""
        else:
            return f"{dt.strftime('%a %b %-d')}{time_str}"
    except (ValueError, TypeError):
        return due_date_str


def _fmt_task_line(t: dict, now_manila: datetime.datetime) -> str:
    date_label = _fmt_task_date(t["due_date"], now_manila)
    middle = f" — {_esc(date_label)}" if date_label else ""
    notify = t.get("notify") or t.get("owner") or ""
    meta = f"Owner: {_esc(t['owner'])}, Notify: {_esc(notify)}"
    return f"  • <b>{_esc(t['task'])}</b>{middle} ({meta})"


def _days_overdue(due_date_str: str, now_manila: datetime.datetime) -> int:
    if not due_date_str:
        return 0
    try:
        dt = datetime.datetime.fromisoformat(due_date_str)
        if dt.tzinfo is None:
            dt = MANILA_TZ.localize(dt)
        else:
            dt = dt.astimezone(MANILA_TZ)
        return max(0, (now_manila.date() - dt.date()).days)
    except (ValueError, TypeError):
        return 0


async def build_digest_text(person: str | None = None) -> str:
    import notion
    now_manila = datetime.datetime.now(MANILA_TZ)
    today_start = now_manila.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_end = today_start + timedelta(days=2) - timedelta(seconds=1)
    week_end = today_start + timedelta(days=7)
    month_end = today_start + timedelta(days=30)

    today_end = today_start + timedelta(days=1) - timedelta(seconds=1)
    tomorrow_start = today_start + timedelta(days=1)

    def _filter(tasks: list) -> list:
        if not person or person == "Both":
            return tasks
        return [t for t in tasks if t.get("notify") in (person, "Both")]

    def _sort_by_due(tasks: list) -> list:
        def _key(t):
            try:
                return datetime.datetime.fromisoformat(t["due_date"])
            except Exception:
                return datetime.datetime.max.replace(tzinfo=timezone.utc)
        return sorted(tasks, key=_key)

    overdue_tasks = _filter(_sort_by_due(notion.get_overdue_tasks()))
    today_tasks = _filter(_sort_by_due(notion.get_tasks_due(
        today_start.astimezone(timezone.utc),
        today_end.astimezone(timezone.utc)
    )))
    tomorrow_tasks = _filter(_sort_by_due(notion.get_tasks_due(
        tomorrow_start.astimezone(timezone.utc),
        tomorrow_end.astimezone(timezone.utc)
    )))
    week_tasks = _filter(_sort_by_due(notion.get_tasks_due(
        (tomorrow_end + timedelta(seconds=1)).astimezone(timezone.utc),
        week_end.astimezone(timezone.utc)
    )))
    month_tasks = _filter(_sort_by_due(notion.get_tasks_due(
        (week_end + timedelta(seconds=1)).astimezone(timezone.utc),
        month_end.astimezone(timezone.utc)
    )))

    db_id = TASKS_DB_ID.replace("-", "")
    notion_link = f"https://www.notion.so/{db_id}"
    date_str = now_manila.strftime("%A, %B %-d")
    if person == "Both":
        header = f"Here's what's on both your plates — <b>{date_str}</b>."
    elif person:
        header = f"Here's what's on your plate, <b>{_esc(person)}</b> — {date_str}."
    else:
        header = f"Here's your briefing for <b>{date_str}</b>."
    lines = [header]

    if overdue_tasks:
        lines.append(f"\n{_DIVIDER}\n<code>⚠️ OVERDUE:</code>")
        for t in overdue_tasks:
            days = _days_overdue(t["due_date"], now_manila)
            suffix = f" — {days}d overdue" if days else " — overdue"
            lines.append(f"  • <b>{_esc(t['task'])}</b>{suffix} ({_esc(t['owner'])})")

    lines.append(f"\n{_DIVIDER}\n<code>TODAY:</code>")
    if today_tasks:
        for t in today_tasks:
            lines.append(_fmt_task_line(t, now_manila))
    else:
        lines.append("  Nothing scheduled.")

    lines.append(f"\n{_DIVIDER}\n<code>TOMORROW:</code>")
    if tomorrow_tasks:
        for t in tomorrow_tasks:
            lines.append(_fmt_task_line(t, now_manila))
    else:
        lines.append("  Nothing scheduled.")

    if week_tasks:
        lines.append(f"\n{_DIVIDER}\n<code>THIS WEEK:</code>")
        for t in week_tasks:
            lines.append(_fmt_task_line(t, now_manila))

    if month_tasks:
        lines.append(f"\n{_DIVIDER}\n<code>THIS MONTH:</code>")
        for t in month_tasks[:5]:
            lines.append(_fmt_task_line(t, now_manila))
        if len(month_tasks) > 5:
            lines.append(f"  + {len(month_tasks) - 5} more — {notion_link}")

    return "\n".join(lines).strip()


async def send_todo_nudges(context: ContextTypes.DEFAULT_TYPE) -> None:
    import notion
    now_manila = datetime.datetime.now(MANILA_TZ)
    today_start = now_manila.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1) - timedelta(seconds=1)
    week_end = today_start + timedelta(days=7)

    try:
        overdue = notion.get_overdue_todos()
        due_today = notion.get_pending_todos(
            today_start.astimezone(timezone.utc),
            today_end.astimezone(timezone.utc)
        )
        due_this_week = notion.get_pending_todos(
            (today_end + timedelta(seconds=1)).astimezone(timezone.utc),
            week_end.astimezone(timezone.utc)
        )
    except Exception as e:
        logger.error(f"send_todo_nudges query failed: {e}")
        return

    async def nudge(task: dict, text: str) -> None:
        owner_id = JAIME_ID if task["owner"] == "Jaime" else DENISE_ID
        try:
            await context.bot.send_message(chat_id=owner_id, text=text, parse_mode=ParseMode.HTML)
            logger.info(f"To-do nudge sent: '{task['task']}' for {task['owner']}")
        except Exception as e:
            logger.warning(f"Could not nudge {task['owner']} for '{task['task']}': {e}")

    for task in overdue:
        days = _days_overdue(task["due_date"], now_manila)
        suffix = f"{days}d overdue" if days else "overdue"
        await nudge(task, f"Still open ({suffix}): <b>{_esc(task['task'])}</b>. Reply 'done' to close it out.")

    for task in due_today:
        await nudge(task, f"Due today: <b>{_esc(task['task'])}</b>. Any progress? Reply 'done' once it's handled.")

    for task in due_this_week:
        due_label = _fmt_task_date(task["due_date"], now_manila)
        await nudge(task, f"Due {_esc(due_label)}: <b>{_esc(task['task'])}</b>. Reply 'done' once it's handled.")


async def send_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        text = await build_digest_text()
        await context.bot.send_message(chat_id=GROUP_ID, text=text, parse_mode=ParseMode.HTML)
        logger.info("Daily digest sent")
    except Exception as e:
        logger.error(f"send_daily_digest failed: {e}")
