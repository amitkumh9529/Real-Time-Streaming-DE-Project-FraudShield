# Databricks notebook source
# DBTITLE 1,Create test email body
body = """
<html>
<body>

<h2>Test Email from Databricks</h2>

<p>This is a test email sent from Databricks notebook.</p>

<p>If you receive this, the email configuration is working correctly.</p>

<br>

<p>
Thanks<br>
<b>FraudShield Support Team</b>
</p>

</body>
</html>
"""

print(body)

# COMMAND ----------

# DBTITLE 1,Create test subject
subject = "Test Email from Databricks"

print(subject)

# COMMAND ----------

gmail_api_key = dbutils.secrets.get(
            scope="fraudshield-scope",
            key="gmail_api_key"
        )

# COMMAND ----------

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Sender and receiver retrieved safely from environment / secrets
EMAIL = os.getenv("ALERT_EMAIL_FROM", "alerts@example.com")
APP_PASSWORD = gmail_api_key

to_email = os.getenv("ALERT_EMAIL_TO", "test-receiver@example.com")

# Create message
msg = MIMEMultipart()
msg["From"] = EMAIL
msg["To"] = to_email
msg["Subject"] = subject

msg.attach(MIMEText(body, "html"))

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL, APP_PASSWORD)
        server.send_message(msg)

    print("✅ Email sent successfully!")

except Exception as e:
    print("❌ Error sending email:", e)
