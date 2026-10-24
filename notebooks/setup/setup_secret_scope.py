# Databricks notebook source
# ==============================================================================
# FraudShield Secret Scope Setup & Secure Credential Provisioning
#
# Securely provisions the `fraudshield-scope` Databricks Secret Scope using
# Databricks REST API. Credentials must be passed as environment variables or
# Databricks widgets, NEVER hardcoded in the script.
# ==============================================================================

import json
import os

import requests

# COMMAND ----------
# 1. Workspace Context & API Configuration

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

DATABRICKS_INSTANCE = api_url
DATABRICKS_TOKEN = api_token

secret_scope_name = os.getenv("SECRET_SCOPE_NAME", "fraudshield-scope")

# COMMAND ----------
# 2. Create Secret Scope (if not existing)

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

create_scope_url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"
payload = {"scope": secret_scope_name}

response = requests.post(create_scope_url, headers=headers, json=payload)
if response.status_code == 200:
    print(f"✅ Secret scope '{secret_scope_name}' created successfully.")
elif response.status_code == 400 and "RESOURCE_ALREADY_EXISTS" in response.text:
    print(f"ℹ️ Secret scope '{secret_scope_name}' already exists. Proceeding.")
else:
    print(f"⚠️ Notice: Scope creation returned status {response.status_code}")

# COMMAND ----------
# 3. Securely Provision Kafka Connection Details
# Credentials must be set in your execution environment:
#   KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_KEY, KAFKA_API_SECRET

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pkc-sample.asia-south2.gcp.confluent.cloud:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "credit_card_transactions")
kafka_api_key = os.getenv("KAFKA_API_KEY", "")
kafka_api_secret = os.getenv("KAFKA_API_SECRET", "")

if not kafka_api_key or not kafka_api_secret:
    print("⚠️ KAFKA_API_KEY or KAFKA_API_SECRET environment variables not set.")
    print("   Please configure them before running this in production.")

kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})

put_secret_url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

put_payload = {
    "scope": secret_scope_name,
    "key": "kafka_connection_details",
    "string_value": kafka_connection_details
}

resp_kafka = requests.post(put_secret_url, headers=headers, json=put_payload)
if resp_kafka.status_code == 200:
    print(f"✅ Secret 'kafka_connection_details' stored securely in scope '{secret_scope_name}'.")
else:
    print(f"❌ Failed to store kafka credentials (Status: {resp_kafka.status_code})")

# COMMAND ----------
# 4. Securely Provision Notification SMTP Credentials
# Set GMAIL_APP_PASSWORD in environment or prompt securely:

gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "")
gmail_from_email = os.getenv("ALERT_EMAIL_FROM", "alerts@example.com")

if gmail_app_password:
    put_pwd_payload = {
        "scope": secret_scope_name,
        "key": "gmail_api_key",
        "string_value": gmail_app_password
    }
    resp_pwd = requests.post(put_secret_url, headers=headers, json=put_pwd_payload)
    if resp_pwd.status_code == 200:
        print(f"✅ Secret 'gmail_api_key' stored securely in scope '{secret_scope_name}'.")
else:
    print("ℹ️ GMAIL_APP_PASSWORD not set in environment. Skipping SMTP secret provisioning.")

# COMMAND ----------
# 5. Verify Secret Retrieval Without Exposing Values

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key="kafka_connection_details"
    )
    parsed = json.loads(retrieved_json)
    # Mask API key before logging
    masked_key = parsed.get("api_key", "")[:4] + "****" if parsed.get("api_key") else "UNSET"
    print(f"✅ Verified retrieval: Topic '{parsed.get('topic')}', Server '{parsed.get('bootstrap_servers')}', API Key: '{masked_key}'")
except Exception as e:
    print(f"❌ Secret verification failed: {e}")
