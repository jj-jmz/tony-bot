from __future__ import annotations

import html
import os
import logging
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from claude_client import parse_intent
import notion

logger = logging.getLogger(__name__)

JAIME_ID = int(os.environ.get("TELEGRAM_JAIME_ID", 110907401))
DENISE_ID = int(os.environ.get("TELEGRAM_DENISE_ID", 219845668))
GROUP_ID = int(os.environ.get("TELEGRAM_GROUP_ID", -1003856456479))

# pending_confirmations[user_id] = {"action": str, "task_id": str, "task_name": str, ...}
pending_confirmations: dict[int, dict] = {}

_CONFIRMATIONS = {"yes", "yeah", "yep", "confirm", "correct", "ok", "okay", "sure", "do it", "confirmed", "yup", "please", "go ahead", "yes please", "yeah please", "do it please", "sounds good", "looks good", "perfect", "great"}
_CANCELLATIONS = {"no", "nope", "cancel", "nevermind", "never mind", "stop", "abort", "nah", "forget it", "forget that", "drop it", "skip it", "leave it", "ignore that", "don't", "dont", "actually no", "actually never mind", "scratch that", "disregard"}


def _esc(s) -> str:
    return html.escape(str(s)) if s else ""


def _is_cancellation(text: str) -> bool:
    lower = text.lower().strip()
    if lower in _CANCELLATIONS:
        return True
    for c in _CANCELLATIONS:
        if lower.startswith(c + " "):
            if len(lower[len(c):].strip().split()) <= 1:
                return True
    return False


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message or not message.text:
        return

    sender_id = message.from_user.id
    if sender_id not in (JAIME_ID, DENISE_ID):
        return

    text = message.text.strip()

    if sender_id in pending_confirmations:
        lower = text.lower()
        pending = pending_confirmations[sender_id]

        if _is_cancellation(lower):
            del pending_confirmations[sender_id]
            from claude_client import conversation_state
            conversation_state.pop(sender_id, None)
            await message.reply_text("Noted. Standing down.", parse_mode=ParseMode.HTML)
            return
        if pending.get("action") == "select_task":
            await _handle_selection(update, context, sender_id, text)
            return
        if lower in _CONFIRMATIONS or any(lower.startswith(c) for c in _CONFIRMATIONS):
            await _execute_pending(update, context, sender_id)
            return

    parsed = parse_intent(text, sender_id)
    logger.info(f"[{sender_id}] intent={parsed.get('intent')} task={parsed.get('task')!r}")
    await _route(parsed, update, context)


async def _route(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    dispatch = {
        "SET_REMINDER": _handle_set_reminder,
        "CLARIFY": _handle_clarify,
        "MARK_DONE": _handle_mark_done,
        "CONFIRM_ACTION": _handle_confirm_action,
        "UPDATE_TASK": _handle_update_task,
        "STATUS_REQUEST": _handle_status_request,
        "DIGEST": _handle_digest,
        "REQUEST_UPDATE": _handle_request_update,
        "SNOOZE": _handle_snooze,
        "DELETE_TASK": _handle_delete_task,
        "UNKNOWN": _handle_unknown,
    }
    handler = dispatch.get(data.get("intent", "UNKNOWN"), _handle_unknown)
    await handler(data, update, context)


async def _handle_set_reminder(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    task = data.get("task")
    owner = data.get("owner")
    due_date = data.get("due_date")
    notify = data.get("notify") or owner
    created_by = data.get("created_by")
    source = _source(update)

    if not all([task, owner, due_date]):
        await update.message.reply_text("A required field is still missing. Please try again.", parse_mode=ParseMode.HTML)
        return

    try:
        task_type = data.get("type") or "To-do"
        group = data.get("group") or None

        conflict_warning = ""
        if task_type == "Event" and due_date:
            persons = [owner]
            if notify == "Both":
                persons.append("Denise" if owner == "Jaime" else "Jaime")
            try:
                conflicts = notion.check_event_conflicts(due_date, persons)
                if conflicts:
                    lines = "\n".join(f"  • <b>{_esc(c['task'])}</b> ({_esc(c['owner'])}, {_esc(_fmt_due(c['due_date']))})" for c in conflicts)
                    conflict_warning = f"\n\n⚠️ <b>Potential scheduling conflict:</b>\n{lines}"
            except Exception as ce:
                logger.warning(f"Conflict check failed: {ce}")

        notion.create_task(task, owner, notify, due_date, source, created_by, task_type, group)
        await update.message.reply_text(
            (data.get("message_to_user") or f"Noted. <b>{_esc(task)}</b> is logged.") + conflict_warning,
            parse_mode=ParseMode.HTML
        )

        if notify == "Both" and update.message.chat_id != GROUP_ID:
            other_id = DENISE_ID if update.message.from_user.id == JAIME_ID else JAIME_ID
            try:
                await context.bot.send_message(
                    chat_id=other_id,
                    text=f"For your awareness: {_esc(created_by)} set a reminder — <b>{_esc(task)}</b> — due {_esc(due_date)}.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as notify_err:
                logger.warning(f"Could not notify other user ({other_id}): {notify_err}")
    except Exception as e:
        logger.error(f"create_task failed: {e}")
        await update.message.reply_text("I ran into a Notion error. Please try again in a moment.", parse_mode=ParseMode.HTML)


async def _handle_clarify(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        data.get("message_to_user", "A few things are missing. Could you clarify?"),
        parse_mode=ParseMode.HTML
    )


async def _handle_mark_done(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    owner = data.get("owner") or _sender_name(sender_id)
    task_name = data.get("task")

    results = notion.search_tasks(task_name, owner, ["Pending", "In Progress"])
    if not results:
        await update.message.reply_text(
            f"I couldn't find an active task matching <b>{_esc(task_name)}</b> for {_esc(owner)}. "
            "Try a different name or check the status in Notion.",
            parse_mode=ParseMode.HTML
        )
        return

    if len(results) == 1:
        task = results[0]
        pending_confirmations[sender_id] = {
            "action": "mark_done",
            "task_id": task["id"],
            "task_name": task["task"],
            "owner": owner,
        }
        await update.message.reply_text(
            data.get("message_to_user") or
            f"Found it — <b>{_esc(task['task'])}</b> (<i>{_esc(task['status'])}</i>). Mark as done? Reply yes to confirm.",
            parse_mode=ParseMode.HTML
        )
    else:
        candidates = results[:5]
        listing = "\n".join(
            f"{i+1}. <b>{_esc(t['task'])}</b> — <i>{_esc(t['status'])}</i>" + (f", due {_esc(_fmt_due(t['due_date']))}" if t.get('due_date') else "")
            for i, t in enumerate(candidates)
        )
        pending_confirmations[sender_id] = {
            "action": "select_task",
            "next_action": "mark_done",
            "candidates": candidates,
            "owner": owner,
        }
        await update.message.reply_text(
            f"A few matches came up. Reply with the number:\n{listing}",
            parse_mode=ParseMode.HTML
        )


async def _handle_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, sender_id: int, text: str) -> None:
    pending = pending_confirmations[sender_id]
    candidates = pending["candidates"]
    try:
        choice = int(text.strip()) - 1
        if not (0 <= choice < len(candidates)):
            raise ValueError
    except ValueError:
        listing = "\n".join(f"{i+1}. <b>{_esc(t['task'])}</b> — <i>{_esc(t['status'])}</i>" for i, t in enumerate(candidates))
        await update.message.reply_text(f"Reply with a number:\n{listing}", parse_mode=ParseMode.HTML)
        return

    task = candidates[choice]
    next_action = pending["next_action"]
    pending_confirmations[sender_id] = {
        "action": next_action,
        "task_id": task["id"],
        "task_name": task["task"],
        "owner": pending["owner"],
    }
    await update.message.reply_text(
        f"Got it — <b>{_esc(task['task'])}</b> (<i>{_esc(task['status'])}</i>). Confirm? Reply yes to proceed.",
        parse_mode=ParseMode.HTML
    )


async def _handle_confirm_action(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    if sender_id in pending_confirmations:
        await _execute_pending(update, context, sender_id)
    else:
        await update.message.reply_text(
            "Nothing pending to confirm — I may have restarted. Could you repeat the request?",
            parse_mode=ParseMode.HTML
        )


async def _handle_update_task(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    owner = data.get("owner") or _sender_name(sender_id)
    task_name = data.get("task")

    results = notion.search_tasks(task_name, owner, ["Pending", "In Progress", "Snoozed"])
    if not results:
        await update.message.reply_text(
            f"No active task matching <b>{_esc(task_name)}</b> found for {_esc(owner)}.",
            parse_mode=ParseMode.HTML
        )
        return

    task = results[0]
    fields: dict = {}
    if data.get("due_date"):
        fields["due_date"] = data["due_date"]
    if data.get("owner") and data["owner"] != "Both":
        fields["owner"] = data["owner"]
    if data.get("notify"):
        fields["notify"] = data["notify"]
    if data.get("notes"):
        fields["notes"] = data["notes"]
    raw_group = data.get("group")
    if raw_group and raw_group.lower() not in ("none", "null", "no group", "n/a"):
        fields["group"] = raw_group
    elif raw_group and raw_group.lower() in ("none", "null", "no group", "n/a"):
        fields["group"] = None

    pending_confirmations[sender_id] = {
        "action": "update_task",
        "task_id": task["id"],
        "task_name": task["task"],
        "fields": fields,
    }
    await update.message.reply_text(
        data.get("message_to_user") or
        f"Ready to update <b>{_esc(task['task'])}</b>. Confirm? Reply yes to proceed.",
        parse_mode=ParseMode.HTML
    )


async def _handle_status_request(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    task_name = data.get("task")
    group = data.get("group")
    owner = data.get("owner") or _sender_name(sender_id)

    if group and not task_name:
        results = notion.get_tasks_by_group(group)
        if not results:
            await update.message.reply_text(f"Nothing active under <b>{_esc(group)}</b>.", parse_mode=ParseMode.HTML)
            return
        lines = [f"<b>{_esc(group)}</b>\n"]
        for r in results:
            status_line = f"  • <b>{_esc(r['task'])}</b> — <i>{_esc(r['status'])}</i> ({_esc(r['owner'])})"
            if r.get("notes"):
                status_line += f"\n    <i>{_esc(r['notes'])}</i>"
            lines.append(status_line)
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return

    results = notion.search_tasks(task_name, owner, ["Pending", "In Progress", "Snoozed", "Awaiting Confirmation"])
    if not results:
        await update.message.reply_text(
            data.get("message_to_user") or f"No active task matching <b>{_esc(task_name)}</b> found.",
            parse_mode=ParseMode.HTML
        )
        return

    if len(results) > 1:
        listing = "\n".join(
            f"• <b>{_esc(r['task'])}</b> — <i>{_esc(r['status'])}</i>" + (f", due {_esc(_fmt_due(r['due_date']))}" if r.get('due_date') else "")
            for r in results[:5]
        )
        await update.message.reply_text(f"Multiple matches found:\n{listing}", parse_mode=ParseMode.HTML)
        return

    result = results[0]
    msg = f"<b>{_esc(result['task'])}</b> — <i>{_esc(result['status'])}</i>, owned by {_esc(result['owner'])}"
    if result.get("due_date"):
        msg += f", due {_esc(_fmt_due(result['due_date']))}"
    if result.get("notes"):
        msg += f"\n<i>{_esc(result['notes'])}</i>"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def _handle_digest(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from scheduler import build_digest_text
    try:
        person = data.get("owner") or None
        text = await build_digest_text(person=person)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"_handle_digest failed: {e}")
        await update.message.reply_text("Couldn't pull the briefing right now. Try again in a moment.", parse_mode=ParseMode.HTML)


async def _handle_request_update(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    task_name = data.get("task")
    owner = data.get("owner")

    result = notion.get_task_status(task_name, owner)
    if not result:
        await update.message.reply_text(
            f"Couldn't find <b>{_esc(task_name)}</b> to send an update request.",
            parse_mode=ParseMode.HTML
        )
        return

    requester = _sender_name(sender_id)
    notion.log_update_request(result["id"], requester)

    other_id = DENISE_ID if sender_id == JAIME_ID else JAIME_ID
    try:
        await context.bot.send_message(
            chat_id=other_id,
            text=f"{_esc(requester)} is requesting a status update on <b>{_esc(result['task'])}</b>. Any progress to report?",
            parse_mode=ParseMode.HTML
        )
    except Exception as notify_err:
        logger.warning(f"Could not notify other user ({other_id}): {notify_err}")
    await update.message.reply_text(
        data.get("message_to_user") or f"Update request sent for <b>{_esc(result['task'])}</b>.",
        parse_mode=ParseMode.HTML
    )


async def _handle_snooze(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    owner = data.get("owner") or _sender_name(sender_id)
    task_name = data.get("task")
    new_due = data.get("due_date")

    results = notion.search_tasks(task_name, owner, ["Pending", "In Progress"])
    if not results:
        await update.message.reply_text(
            f"No active task matching <b>{_esc(task_name)}</b> found.",
            parse_mode=ParseMode.HTML
        )
        return

    task = results[0]
    if new_due:
        notion.update_task_fields(task["id"], {"due_date": new_due})
        notion.update_task_status(task["id"], "Snoozed")
        await update.message.reply_text(
            data.get("message_to_user") or f"<b>{_esc(task['task'])}</b> snoozed to {_esc(_fmt_due(new_due))}.",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            "What date and time should I reschedule this to? Reply with both.",
            parse_mode=ParseMode.HTML
        )


async def _handle_delete_task(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    owner = data.get("owner") or _sender_name(sender_id)
    task_name = data.get("task")

    results = notion.search_tasks(task_name, owner, ["Pending", "In Progress", "Snoozed", "Awaiting Confirmation"])
    if not results:
        await update.message.reply_text(
            f"No active task matching <b>{_esc(task_name)}</b> found for {_esc(owner)}. "
            "It may already be done, deleted, or the name might be slightly different — try the digest to see what's current.",
            parse_mode=ParseMode.HTML
        )
        return

    if len(results) == 1:
        task = results[0]
        pending_confirmations[sender_id] = {
            "action": "delete_task",
            "task_id": task["id"],
            "task_name": task["task"],
            "owner": owner,
        }
        await update.message.reply_text(
            f"About to delete <b>{_esc(task['task'])}</b> (<i>{_esc(task['status'])}</i>). This can't be undone. Confirm?",
            parse_mode=ParseMode.HTML
        )
    else:
        candidates = results[:5]
        listing = "\n".join(
            f"{i+1}. <b>{_esc(t['task'])}</b> — <i>{_esc(t['status'])}</i>" + (f", due {_esc(_fmt_due(t['due_date']))}" if t.get('due_date') else "")
            for i, t in enumerate(candidates)
        )
        pending_confirmations[sender_id] = {
            "action": "select_task",
            "next_action": "delete_task",
            "candidates": candidates,
            "owner": owner,
        }
        await update.message.reply_text(
            f"Which one to delete? Reply with the number:\n{listing}",
            parse_mode=ParseMode.HTML
        )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sender_id = update.message.from_user.id
    from claude_client import conversation_state
    cleared = sender_id in pending_confirmations or sender_id in conversation_state
    pending_confirmations.pop(sender_id, None)
    conversation_state.pop(sender_id, None)
    msg = "Slate wiped. What's next?" if cleared else "Nothing in progress to cancel."
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def _handle_unknown(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        data.get("message_to_user") or "I didn't quite catch that. Could you rephrase?",
        parse_mode=ParseMode.HTML
    )


async def _execute_pending(update: Update, context: ContextTypes.DEFAULT_TYPE, sender_id: int) -> None:
    pending = pending_confirmations.pop(sender_id)
    action = pending["action"]
    task_name = pending.get("task_name", "the task")

    try:
        if action == "mark_done":
            notion.update_task_done(pending["task_id"])
            owner = pending.get("owner", "")
            other_id = DENISE_ID if owner == "Jaime" else JAIME_ID
            await update.message.reply_text(
                f"Done. <b>{_esc(task_name)}</b> marked complete.",
                parse_mode=ParseMode.HTML
            )
            try:
                await context.bot.send_message(
                    chat_id=other_id,
                    text=f"For your awareness: <b>{_esc(task_name)}</b> has been marked complete by {_esc(owner)}.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as notify_err:
                logger.warning(f"Could not notify other user ({other_id}): {notify_err}")
        elif action == "update_task":
            notion.update_task_fields(pending["task_id"], pending.get("fields", {}))
            await update.message.reply_text(
                f"Understood. <b>{_esc(task_name)}</b> has been updated.",
                parse_mode=ParseMode.HTML
            )
        elif action == "delete_task":
            notion.delete_task(pending["task_id"])
            await update.message.reply_text(
                f"Done. <b>{_esc(task_name)}</b> has been deleted.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"_execute_pending failed: {e}")
        await update.message.reply_text("Something went wrong. Please try again.", parse_mode=ParseMode.HTML)


def _fmt_due(due_date_str: str) -> str:
    try:
        from datetime import datetime as dt
        import pytz
        d = dt.fromisoformat(due_date_str).astimezone(pytz.timezone("Asia/Manila"))
        return d.strftime("%b %-d, %-I:%M %p") if (d.hour or d.minute) else d.strftime("%b %-d")
    except Exception:
        return due_date_str


def _sender_name(sender_id: int) -> str:
    return "Jaime" if sender_id == JAIME_ID else "Denise"


def _source(update: Update) -> str:
    if update.message.chat_id == GROUP_ID:
        return "Group"
    return "Jaime-DM" if update.message.from_user.id == JAIME_ID else "Denise-DM"
