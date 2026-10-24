"""Alert Sink: High Value Transaction Email Notifier.

Processes high-value limit violation alerts and dispatches warning notifications.
Implements bounded micro-batch processing, secret scope credential retrieval,
idempotency tracking, and strict PCI-DSS masking.
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)


def send_email(to_email: str, subject: str, body: str, from_email: str, app_password: str) -> None:
    """Send an HTML email securely using TLS SMTP."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def create_alert_email_body(alert_data: dict) -> str:
    """Generate responsive HTML body for high-value limit alert."""
    return f"""<html><body style="font-family: Arial, sans-serif; background-color: #f8fafc; padding: 20px;">
<div style="max-width: 600px; margin: auto; background: #fff; padding: 24px; border-radius: 8px; border: 1px solid #e2e8f0;">
  <h2 style="color: #d9534f; margin-top: 0;">⚠️ High Value Transaction Alert</h2>
  <p>Dear {alert_data['customer_name']},</p>
  <p>We detected a transaction that exceeded your configured spending limit:</p>

  <div style="background-color: #f8f9fa; padding: 14px; border-left: 4px solid #d9534f; margin: 16px 0;">
    <h3 style="margin-top: 0;">Transaction Details</h3>
    <ul style="list-style-type: none; padding-left: 0; margin-bottom: 0;">
      <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
      <li><strong>Amount:</strong> {alert_data['currency']} {alert_data['transaction_amount']:,.2f}</li>
      <li><strong>Configured Limit:</strong> {alert_data['currency']} {alert_data['transaction_limit']:,.2f}</li>
      <li><strong>Card:</strong> {alert_data['card_number_masked']}</li>
      <li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
      <li><strong>Location:</strong> {alert_data['city']}, {alert_data['country']}</li>
      <li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
    </ul>
  </div>

  <p style="font-size: 13px; color: #6c757d; border-top: 1px solid #eee; padding-top: 12px; margin-top: 20px;">
    Alert ID: {alert_data['alert_id']}<br>
    This is an automated notification from FraudShield Transaction Monitoring System.
  </p>
</div>
</body></html>"""


# Retrieve configuration securely from Databricks Secret Scope or Environment
try:
    APP_PASSWORD = dbutils.secrets.get("fraudshield-scope", "gmail_api_key")
except Exception:
    APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", None)

try:
    EMAIL_FROM = dbutils.secrets.get("fraudshield-scope", "alert_email_from")
except Exception:
    EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "alerts-noreply@fraudshield.internal")

_processed_hv_alert_cache: set[str] = set()


@dp.foreach_batch_sink(name="email_notifier_sink")
def send_high_value_alert_emails(df: DataFrame, batch_id: int) -> None:
    """Micro-batch sink dispatching alerts with bounded processing and deduplication."""
    if APP_PASSWORD is None:
        logger.warning("Batch %s: SMTP key not available; skipping email notifications", batch_id)
        return

    batch_records = df.limit(500).collect()
    logger.info("Batch %s: Processing %d high-value alert(s)...", batch_id, len(batch_records))

    success_count = 0
    failure_count = 0

    for row in batch_records:
        alert_id = getattr(row, "alert_id", None)
        if not alert_id or alert_id in _processed_hv_alert_cache:
            continue

        try:
            card_masked = getattr(row, "card_last_four", "****")
            alert_data = {
                "alert_id": alert_id,
                "customer_name": getattr(row, "customer_name", "Valued Customer"),
                "transaction_id": getattr(row, "transaction_id", "N/A"),
                "card_number_masked": card_masked,
                "transaction_amount": float(getattr(row, "transaction_amount", 0.0)),
                "transaction_limit": float(getattr(row, "transaction_limit", 0.0)),
                "currency": getattr(row, "currency", "INR"),
                "merchant_name": getattr(row, "merchant_name", "N/A"),
                "merchant_category": getattr(row, "merchant_category", "N/A"),
                "city": getattr(row, "city", "N/A"),
                "country": getattr(row, "country", "N/A"),
                "transaction_timestamp": str(getattr(row, "transaction_timestamp", "")),
            }

            subject = f"⚠️ High Value Transaction Alert: {alert_data['currency']} {alert_data['transaction_amount']:,.2f} [{alert_data['alert_id']}]"
            body = create_alert_email_body(alert_data)
            recipient = getattr(row, "customer_email", None) or os.getenv("ALERT_EMAIL_TO", "alerts-triage@fraudshield.internal")

            send_email(recipient, subject, body, EMAIL_FROM, APP_PASSWORD)
            _processed_hv_alert_cache.add(alert_id)
            success_count += 1
        except Exception as e:
            failure_count += 1
            logger.error("Error processing high-value alert %s: %s", alert_id, e)

    logger.info("Batch %s complete: %d succeeded, %d failed", batch_id, success_count, failure_count)


@dp.append_flow(target="email_notifier_sink")
def high_value_transaction_stream():
    """Streaming flow that reads high-value alerts."""
    return spark.readStream.table("fraudshield.gold.high_value_transactions_alert")
