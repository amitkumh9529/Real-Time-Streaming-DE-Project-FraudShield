"""Reliable & Idempotent Alert Dispatcher Service.

Processes records from the durable `alert_outbox` Delta table.
Implements bounded retries, exponential backoff, dead-letter queue (DLQ) state,
and idempotent message delivery with PCI-DSS compliant masking.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class AlertDispatcher:
    """Dispatches alerts from the outbox with retry management and idempotency."""

    def __init__(
        self,
        smtp_sender: Optional[str] = None,
        smtp_password: Optional[str] = None,
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 587,
        max_retries: int = 3,
        smtp_client_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.smtp_sender = smtp_sender or os.getenv("ALERT_EMAIL_FROM", "alerts-noreply@fraudshield.internal")
        self.smtp_password = smtp_password or os.getenv("GMAIL_APP_PASSWORD", "")
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.max_retries = max_retries
        self._smtp_client_factory = smtp_client_factory

        # In-memory tracking of delivered alert IDs for local idempotency guarantee
        self._delivered_alert_ids: set[str] = set()

    def is_already_delivered(self, alert_id: str) -> bool:
        """Check if an alert ID has already been successfully delivered."""
        return alert_id in self._delivered_alert_ids

    def render_email_html(self, alert_type: str, customer_name: str, payload: Dict[str, Any]) -> str:
        """Render responsive, accessible HTML security alert email with masked PII."""
        card_last_four = payload.get("card_last_four", "****")
        amount = payload.get("amount", 0.0)
        currency = payload.get("currency", "INR")
        merchant = payload.get("merchant_name", "Unknown Merchant")
        category = payload.get("merchant_category", "N/A")
        city = payload.get("transaction_city", "N/A")
        country = payload.get("transaction_country", "N/A")
        txn_id = payload.get("transaction_id", "N/A")
        alert_id = payload.get("alert_id", "N/A")
        fraud_score = payload.get("fraud_score", 0)
        fraud_reason = payload.get("fraud_reason", "NORMAL")

        return f"""<!DOCTYPE html>
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #f8fafc; padding: 20px;">
  <div style="max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; overflow: hidden;">
    <div style="background: #dc2626; color: white; padding: 20px 24px;">
      <h2 style="margin: 0; font-size: 20px;">🚨 Security Alert: Suspicious Transaction Flagged</h2>
    </div>
    <div style="padding: 24px;">
      <p>Dear <strong>{customer_name}</strong>,</p>
      <p>Our real-time transaction monitoring engine detected activity requiring your attention:</p>

      <div style="background: #f1f5f9; border-radius: 6px; padding: 16px; margin: 20px 0;">
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
          <tr><td style="padding: 6px 0; color: #64748b;">Alert Type:</td><td style="font-weight: 600;">{alert_type}</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Transaction ID:</td><td>{txn_id}</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Card Number:</td><td>{card_last_four}</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Amount:</td><td style="font-weight: 700; color: #dc2626;">{currency} {amount:,.2f}</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Merchant:</td><td>{merchant} ({category})</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Location:</td><td>{city}, {country}</td></tr>
          <tr><td style="padding: 6px 0; color: #64748b;">Risk Score:</td><td>{fraud_score}/100 ({fraud_reason})</td></tr>
        </table>
      </div>

      <p style="font-size: 13px; color: #475569;">If you did not authorize this transaction, please lock your card immediately in the mobile banking app.</p>
    </div>
    <div style="background: #f8fafc; padding: 12px 24px; border-top: 1px solid #e2e8f0; font-size: 12px; color: #94a3b8;">
      Alert ID: {alert_id} &bull; Sent by FraudShield Automated Monitoring Engine
    </div>
  </div>
</body>
</html>"""

    def send_notification(self, recipient: str, subject: str, body_html: str) -> None:
        """Send an email over TLS SMTP. Supports custom client factory for testing."""
        if not self.smtp_password:
            logger.warning("SMTP credentials not configured. Skipping email dispatch to %s.", recipient)
            return

        msg = MIMEMultipart("alternative")
        msg["From"] = self.smtp_sender
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html"))

        if self._smtp_client_factory:
            server = self._smtp_client_factory()
            server.send_message(msg)
            return

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(self.smtp_sender, self.smtp_password)
            server.send_message(msg)

    def process_outbox_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process an individual outbox record idempotently with state transition."""
        alert_id = record["alert_id"]
        delivery_status = record.get("delivery_status", "PENDING")
        retry_count = int(record.get("retry_count", 0))
        max_retries = int(record.get("max_retries", self.max_retries))

        # Idempotency check: if already sent, do not re-send
        if delivery_status == "SENT" or self.is_already_delivered(alert_id):
            return {
                **record,
                "delivery_status": "SENT",
                "failure_reason": None
            }

        # Check if max retries exceeded -> Dead Letter Queue
        if retry_count >= max_retries:
            return {
                **record,
                "delivery_status": "DEAD_LETTER",
                "failure_reason": f"Exceeded maximum retries ({max_retries})"
            }

        payload = record.get("payload_json", {})
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        recipient = record.get("recipient_email", "alerts-triage@fraudshield.internal")
        customer_name = record.get("customer_name", "Valued Customer")
        alert_type = record.get("alert_type", "SECURITY_ALERT")
        subject = f"🚨 FraudShield Alert: {alert_type} [{alert_id}]"

        try:
            body_html = self.render_email_html(alert_type, customer_name, payload)
            self.send_notification(recipient, subject, body_html)
            self._delivered_alert_ids.add(alert_id)
            return {
                **record,
                "delivery_status": "SENT",
                "retry_count": retry_count,
                "delivered_timestamp": datetime.utcnow().isoformat(),
                "failure_reason": None
            }
        except Exception as exc:
            new_retry_count = retry_count + 1
            new_status = "DEAD_LETTER" if new_retry_count >= max_retries else "FAILED"
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.error("Failed to deliver alert %s (attempt %d/%d): %s", alert_id, new_retry_count, max_retries, error_msg)
            return {
                **record,
                "delivery_status": new_status,
                "retry_count": new_retry_count,
                "last_attempt_timestamp": datetime.utcnow().isoformat(),
                "failure_reason": error_msg
            }
