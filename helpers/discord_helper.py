import os
import requests
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook
from typing import List, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

# ================================ Environment Variables ========================
load_dotenv("./envs/.env.sender")
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
# ===============================================================================

def _log_response(response: requests.Response, logger, success_msg: str, failure_msg: str) -> None:
    if 200 <= response.status_code < 300:
        logger.info(f"[Discord] {success_msg} Status: {response.status_code}")
    else:
        logger.error(f"[Discord] {failure_msg} Status: {response.status_code}, Reason: {response.text}")

def discord_ping(logger) -> None:
    """
    Ping the Discord webhook to check if it's reachable.
    """
    try:
        response = requests.post(url=DISCORD_WEBHOOK_URL)
        _log_response(response, logger, "Ping successful.", "Ping failed.")
    except Exception as e:
        logger.error(f"[Discord] Ping error: {e}")

def discord_alert_message(
    title: str,
    detail: str,
    logger,
    alert_type: str = "error"  # "error", "warning", "success"
) -> None:
    """
    Sends a formatted alert message to Discord via webhook.
    
    alert_type: 
        - "error"   -> :x:
        - "warning" -> ⚠️
        - "success" -> ✅
    """
    if not DISCORD_WEBHOOK_URL:
        logger.error("[Discord] Webhook URL is not set.")
        return
    
    icons = {
        "error": ":x:",
        "warning": "⚠️",
        "success": "✅"
    }
    icon = icons.get(alert_type.lower(), ":grey_question:")
    
    runtime = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
    content = f"{icon} {runtime} - **{title}**\n```\n{detail}\n```"
    
    try:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)
        webhook.execute()
        logger.info(f"[Discord] Sent {alert_type} alert for: {title}")
    except Exception as e:
        logger.error(f"[Discord] Failed to send {alert_type} alert: {e}")


def discord_send_message(title: str, message: str, logger) -> None:
    """
    Send a plain text message to the Discord webhook.
    """
    content = f"**{title}**\n\n{message}"
    webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)

    try:
        response = webhook.execute()
        _log_response(response, logger, f"Message sent: {content[:50]}...", "Message failed to send.")
    except Exception as e:
        logger.error(f"[Discord] Message send error: {e}")

def discord_send_attachments(title: str, message: str, attachments: Optional[List[str]], logger) -> None:
    """
    Send a message with file attachments to the Discord webhook.
    """
    content = f"**{title}**\n\n{message}"
    webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)

    try:
        total_size = 0
        for file_path in attachments or []:
            file_size = os.path.getsize(file_path)
            total_size += file_size

            if file_size > 8 * 1024 * 1024:
                logger.warning(f"File '{file_path}' exceeds Discord's 8MB limit and will not be sent.")
                return

            with open(file_path, "rb") as f:
                webhook.add_file(file=f.read(), filename=os.path.basename(file_path))

        if total_size > 8 * 1024 * 1024:
            logger.warning("Total attachment size exceeds Discord's 8MB limit.")
            return

        response = webhook.execute()
        _log_response(response, logger, "Message with attachments sent.", "Failed to send attachments.")
    except Exception as e:
        logger.error(f"[Discord] Attachment send error: {e}")

