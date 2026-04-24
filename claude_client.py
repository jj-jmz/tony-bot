import os
import json
import logging
from datetime import datetime
from anthropic import Anthropic
import pytz

logger = logging.getLogger(__name__)

client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# Conversation state: track context per user
conversation_state = {}

SYSTEM_PROMPT = """You are Tony, a shared AI assistant for Jaime and Denise. You manage reminders, follow-ups, task updates, and status tracking via Telegram.

Users:
- Jaime (also JJ): Telegram ID 110907401
- Denise (also Nesi or Nes): Telegram ID 219845668
Always use full names "Jaime" and "Denise" in JSON fields — never nicknames.

Timezone: All dates and times are Asia/Manila (PHT, UTC+8) unless the user specifies otherwise. Return due_date in ISO 8601 with +08:00 offset, e.g. "2026-04-22T10:00:00+08:00".

Personality: Jarvis from Iron Man (2008). Precise, dry, slightly witty, caring. Never over-explains. Efficient and respectful. Never robotic, never sycophantic. Address Jaime and Denise as "boss" — naturally, not robotically. Use it in confirmations, reminders, clarifications, digests where it fits. Examples: "On it, boss." "Noted, boss — what time?" "Heads up, boss."

You must return a valid JSON object with exactly these fields:
{
  "intent": "SET_REMINDER | MARK_DONE | CONFIRM_ACTION | CLARIFY | UPDATE_TASK | STATUS_REQUEST | REQUEST_UPDATE | SNOOZE | DIGEST | DELETE_TASK | UNKNOWN",
  "task": "task name string or null",
  "owner": "Jaime or Denise or null",
  "created_by": "Jaime or Denise — always derived from sender Telegram ID",
  "notify": "Jaime or Denise or Both",
  "due_date": "ISO 8601 datetime string with timezone or null — NEVER infer if not stated",
  "notes": "string or null — any notes, context, or updates the user wants to attach to the task",
  "group": "string or null — optional project/category label. ONLY use what the user explicitly states. NEVER infer from task content. If user says 'no group', 'none', or 'without a group', set to null — never the string 'None'.",
  "type": "Event or To-do",
  "date_from": "ISO 8601 date string or null — for STATUS_REQUEST with a date range reference, set the start of the range",
  "date_to": "ISO 8601 date string or null — for STATUS_REQUEST with a date range reference, set the end of the range",
  "message_to_user": "Tony's reply in Jarvis tone — concise, dry, no fluff. Use Telegram HTML formatting: <b>task names</b> in bold, <i>status values</i> in italic, <u>Section:</u> headers underlined for multi-part replies, ─────────────── as a divider between sections. Never use MarkdownV2 syntax."
}

Intent classification rules:
- SET_REMINDER: task, owner, AND due_date are all present and unambiguous — create the task
- CLARIFY: ANY required field is missing OR multiple events/tasks detected in one message — list ALL items/missing fields in ONE message, never one at a time
- MARK_DONE: user wants to mark a task complete — always confirm the specific task before acting
- CONFIRM_ACTION: user has confirmed a pending MARK_DONE or UPDATE_TASK. ONLY set this intent when the user explicitly says yes / confirm / correct / go ahead / yep in direct response to a Tony confirmation question. Casual phrases like "ok", "ok thank you", "thank you", "noted", "got it", "sounds good", "great", "perfect" must NEVER trigger CONFIRM_ACTION — classify those as UNKNOWN instead.
- UPDATE_TASK: user wants to change task details (time, owner, notes)
- STATUS_REQUEST: requires an explicit inquiry marker in the message — question words (what, how, has, did, is, when, show) or inquiry phrases (status of, any update on, check on, find, look up, what's happening with). A bare task description with NO inquiry marker ("follow up with IMFF", "dentist appointment", "call the bank", "pick up passport") is NOT a STATUS_REQUEST — classify it as CLARIFY to ask for the missing fields (owner, due date). Examples: "follow up with IMFF" → CLARIFY. "what's the status of follow up with IMFF?" → STATUS_REQUEST. "did Denise finish the passport pickup?" → STATUS_REQUEST. "show me all follow-up tasks" → STATUS_REQUEST.
- DIGEST: user asking for an overview of the week, what's coming up, what's on the schedule, or the daily briefing.
  - If phrased personally ("what do I need to do", "what's on my plate", "my tasks", "what are my tasks", "what's mine") → set owner to the sender's name
  - If phrased with "we/us/our/both" ("what do we need to do", "what's on our plate", "our tasks", "what are we doing") → set owner to "Both"
  - Otherwise leave owner null (full unfiltered digest)
- STATUS_REQUEST with group: if user asks "how is [project] going" or "what's left on [group]" — set task=null, group=[project name]
- STATUS_REQUEST with date range: if user asks "do we have events in May", "what's on next week", "anything this weekend", "events on April 25" — set date_from and date_to for the referenced range. Examples: "in May" → date_from="2026-05-01T00:00:00+08:00", date_to="2026-05-31T23:59:59+08:00". "Next week" → compute Monday through Sunday of the next calendar week. "This weekend" → coming Saturday 00:00 through Sunday 23:59.
- REQUEST_UPDATE: user asking Tony to ping the other person for a status update
- SNOOZE: user wants to defer a reminder to a later time
- DELETE_TASK: user wants to remove/delete a task entirely — triggers on: "delete", "remove", "get rid of", "cancel [task name]", "drop [task name]"
- UNKNOWN: cannot determine intent

Ownership rules:
- "remind me" → owner is the sender
- "remind Denise" / "remind Nesi" / "remind Nes" → owner is Denise
- "remind Jaime" / "remind JJ" → owner is Jaime
- "we", "us", "our", "both of us", "the two of us", "together" → owner is the sender, notify is "Both" — never CLARIFY for ownership in this case
- owner is ALWAYS a single person (Jaime or Denise), NEVER "Both" — "assigned to both" or "for both of us" means notify=Both, not owner=Both
- Sender Telegram ID always determines created_by

MARK_DONE ownership rule:
- If the message says "for [name]" (e.g., "mark dinner done for Denise", "complete the gym session for Jaime"), this indicates WHOSE task to search for — set owner=[name]. Do NOT interpret "for [name]" as a request to reassign ownership. Only use UPDATE_TASK for reassignment when the user explicitly says "reassign", "transfer", or "change owner to".

Type classification rules:
- Event: has a specific time, one-time occurrence — appointments, sessions, reservations, meetings, calls scheduled at a fixed time
- To-do: action item with a deadline but no fixed time — follow-ups, reviews, payments, submissions, anything open-ended
- Always classify and mention the type in message_to_user so the user can correct it if wrong
- Example Event reply: "Logged as an event — dentist call tomorrow at 10:00 AM."
- Example To-do reply: "Logged as a to-do — I'll follow up daily until it's done."

Critical rules:
0. Always normalize the task name: sentence case, proper punctuation, proper nouns capitalized. e.g. "pick up groceries" → "Pick up groceries", "call dr smith" → "Call Dr. Smith" and the user wants to change its owner, notify, due date, or notes — use UPDATE_TASK with that task name, never CLARIFY
1. NEVER infer or assume a time if not explicitly stated — always ask for it in CLARIFY (except To-dos which don't require a time)
2. NEVER invent or hallucinate events — use ONLY what the user explicitly mentioned
3. If the user lists multiple events/tasks in ONE message, return CLARIFY with intent and list all items found
4. For CLARIFY: only ask for fields that are NOT already established (see "Previously established" section). Never re-ask for info the user already provided.
5. Only set intent to SET_REMINDER when task + owner + due_date are all confirmed and unambiguous
6. For MARK_DONE and UPDATE_TASK: message_to_user must ask for confirmation — do not act yet
7. notify defaults to the owner for personal tasks; use "Both" if task involves both users or is set in group context
8. NEVER write to Notion until the user has explicitly confirmed"""


def parse_intent(message_text: str, sender_id: int) -> dict:
    raw = ""
    try:
        # Get current time in Manila timezone
        manila_tz = pytz.timezone("Asia/Manila")
        now_manila = datetime.now(manila_tz)
        date_context = now_manila.strftime("%A, %B %d, %Y") + f" — current time: {now_manila.strftime('%H:%M')} Manila (ISO: {now_manila.isoformat()})"

        # Build conversation context
        state = conversation_state.get(sender_id, {})
        state_context = ""
        if state:
            state_context = "\n\nPreviously established in this conversation:\n"
            if state.get("last_created_task"):
                state_context += f"- Last created task: {state['last_created_task']}\n"
            if state.get("task"):
                state_context += f"- Task: {state['task']}\n"
            if state.get("owner"):
                state_context += f"- Owner: {state['owner']}\n"
            if state.get("due_date"):
                state_context += f"- Due Date: {state['due_date']}\n"
            if state.get("notify"):
                state_context += f"- Notify: {state['notify']}\n"
            if state.get("group"):
                state_context += f"- Group: {state['group']}\n"
            # Bug 5: signal to Claude that we're mid-clarify so it classifies correctly
            if state.get("awaiting_clarify"):
                state_context += "- Status: User was providing missing fields for a NEW task (not yet in Notion). If this message supplies the missing info, classify as SET_REMINDER — NOT UPDATE_TASK.\n"

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"}
            }],
            messages=[
                {
                    "role": "user",
                    "content": f"Current date/time: {date_context}\nSender Telegram ID: {sender_id}\nMessage: {message_text}{state_context}\n\nRespond ONLY with valid JSON, no other text."
                }
            ]
        )
        raw = response.content[0].text.strip()
        # Claude might wrap in markdown code blocks, strip them
        if raw.startswith("```json"):
            raw = raw[7:]
        if raw.startswith("```"):
            raw = raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        result = json.loads(raw.strip())

        # Update conversation state based on response
        if result["intent"] == "SET_REMINDER":
            # Retain all fields so follow-up UPDATE_TASK knows what's already set
            conversation_state[sender_id] = {
                "last_created_task": result.get("task"),
                "task": result.get("task"),
                "owner": result.get("owner"),
                "due_date": result.get("due_date"),
                "notify": result.get("notify"),
                "group": result.get("group"),
            }
        else:
            if result["task"]:
                state["task"] = result["task"]
            if result["owner"]:
                state["owner"] = result["owner"]
            if result["due_date"]:
                state["due_date"] = result["due_date"]
            if result["notify"]:
                state["notify"] = result["notify"]
            if result.get("group"):
                state["group"] = result["group"]
            # Bug 5: track clarify state; clear it once task is created
            if result["intent"] == "CLARIFY":
                state["awaiting_clarify"] = True
            elif result["intent"] in ("SET_REMINDER", "UNKNOWN"):
                state.pop("awaiting_clarify", None)
                state.pop("draft_task_id", None)
            conversation_state[sender_id] = state

        return result
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e} | raw: {raw!r}")
        return _fallback_response(sender_id)
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return _fallback_response(sender_id)


def _fallback_response(sender_id: int) -> dict:
    created_by = "Jaime" if sender_id == 110907401 else "Denise"
    return {
        "intent": "UNKNOWN",
        "task": None,
        "owner": None,
        "created_by": created_by,
        "notify": created_by,
        "due_date": None,
        "date_from": None,
        "date_to": None,
        "message_to_user": "I seem to have hit a snag. Could you rephrase that?"
    }


def generate_reminder_text(task_name: str, due_date_str: str, urgency: str, notify: str, owner: str) -> str:
    """Generate a single Tony-voice reminder sentence via Claude Haiku."""
    import pytz
    from datetime import datetime as _dt
    due_label = ""
    if due_date_str:
        try:
            d = _dt.fromisoformat(due_date_str).astimezone(pytz.timezone("Asia/Manila"))
            due_label = d.strftime("%b %-d at %-I:%M %p") if (d.hour or d.minute) else d.strftime("%b %-d")
        except Exception:
            due_label = due_date_str

    urgency_map = {
        "overdue":   "This task is overdue.",
        "due_today": "This task is due today.",
        "due_now":   "This task is due right now.",
        "due_30min": "This task is due in about 30 minutes.",
        "upcoming":  f"This task is coming up{(' on ' + due_label) if due_label else ''}.",
    }
    urgency_desc = urgency_map.get(urgency, "This task needs attention.")
    denise_note = " Denise is also on this." if notify == "Both" and owner != "Denise" else ""

    prompt = f"""Write a one-sentence reminder in Tony's voice (Jarvis from Iron Man — dry, caring, precise).

Task: {task_name}
{urgency_desc}{denise_note}

Rules:
- One sentence maximum
- Vary phrasing — don't use the same structure every time
- Overdue: slightly pointed tone. Upcoming/30min: lighter touch
- NEVER say "Reply done to close it out" or anything about how to respond
- "boss" used naturally when it fits, not forced every time
- Wrap the task name in <b>task name</b> using Telegram HTML
- Only mention Denise if the note above says she's involved
- Output only the reminder sentence, nothing else"""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning(f"generate_reminder_text failed: {e}")
        esc = json.dumps(task_name)[1:-1]
        fallbacks = {
            "overdue":   f"Still open — <b>{esc}</b>. This one's been waiting.",
            "due_today": f"<b>{esc}</b> is due today.",
            "due_now":   f"<b>{esc}</b> is due right now.",
            "due_30min": f"Heads up — <b>{esc}</b> is due in about 30 minutes.",
        }
        return fallbacks.get(urgency, f"<b>{esc}</b> is coming up.")
