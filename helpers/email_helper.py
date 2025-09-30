import os
import sys
import smtplib
from dotenv import load_dotenv
from typing import Union, List, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ================================ Environment Variables ========================
load_dotenv("./envs/.env.sender")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
# ===============================================================================

def email_send_attachments(
    recipient_email: Union[str, List[str]],
    subject: str,
    body: str,
    attachments: Optional[Union[str, List[str]]] = None,
    logger=None
) -> None:
    """
    Sends an email with the given subject, body, and optional attachments.

    Args:
        recipient_email (str | List[str]): Target recipient(s).
        subject (str): Email subject.
        body (str): Plain text message.
        attachments (str | List[str], optional): File path(s) to attach.

    Returns:
        None. Logs result or exits on error.
    """
    if isinstance(recipient_email, str):
        recipient_email = [recipient_email]

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ", ".join(recipient_email)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        # Attach files
        if attachments:
            if not isinstance(attachments, list):
                attachments = [attachments]

            for file_path in attachments:
                try:
                    with open(file_path, 'rb') as f:
                        file_data = f.read()
                        file_name = os.path.basename(file_path)
                    file_part = MIMEApplication(file_data)
                    file_part.add_header("Content-Disposition", f"attachment; filename={file_name}")
                    msg.attach(file_part)
                except Exception as e:
                    logger.error(f"Failed to attach file '{file_path}': {e}")
                    sys.exit(1)

        # Send email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, recipient_email, msg.as_string())

        logger.info(f"Email successfully sent")

    except Exception as e:
        logger.error(f"Failed to send email")
        sys.exit(1)
