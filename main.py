import os
import logging
from dotenv import load_dotenv

load_dotenv()

from telegram.ext import Application, MessageHandler, CommandHandler, filters
from handlers import handle_message, cancel_command
from scheduler import setup_scheduler, send_daily_digest

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main() -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    webhook_url = os.environ.get("WEBHOOK_URL") or None
    port = int(os.environ.get("PORT") or 8443)

    app = Application.builder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("digest", send_daily_digest))
    app.add_handler(CommandHandler("cancel", cancel_command))

    setup_scheduler(app)

    if webhook_url:
        logger.info(f"Starting webhook on :{port} → {webhook_url}")
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            webhook_url=webhook_url,
            secret_token=os.environ.get("WEBHOOK_SECRET"),
        )
    else:
        logger.info("WEBHOOK_URL not set — running in polling mode")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
