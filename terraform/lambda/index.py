"""
O2 Platform — Slack Notifier Lambda
Triggered by SNS when an anomaly is detected in Kafka Streams.
Sends a formatted alert to a Slack webhook.
"""

import json
import os
import urllib.request


SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

RISK_EMOJI = {
    "CRITICAL": ":rotating_light:",
    "HIGH": ":warning:",
    "MEDIUM": ":large_yellow_circle:",
    "LOW": ":white_circle:",
}


def handler(event, context):
    for record in event.get("Records", []):
        message = record.get("Sns", {}).get("Message", "{}")

        try:
            anomaly = json.loads(message)
        except json.JSONDecodeError:
            anomaly = {"description": message}

        risk_level = anomaly.get("risk_level", "UNKNOWN")
        emoji = RISK_EMOJI.get(risk_level, ":question:")

        slack_message = {
            "text": (
                f"{emoji} *O2 Anomaly Detected*\n"
                f"*Account:* `{anomaly.get('account_id', 'N/A')}`\n"
                f"*Type:* {anomaly.get('event_type', 'N/A')}\n"
                f"*Rule:* {anomaly.get('rule', 'N/A')}\n"
                f"*Risk:* {risk_level} ({anomaly.get('risk_score', 0):.2f})\n"
                f"*Detail:* {anomaly.get('description', 'N/A')}"
            )
        }

        if SLACK_WEBHOOK_URL:
            req = urllib.request.Request(
                SLACK_WEBHOOK_URL,
                data=json.dumps(slack_message).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req)
            print(f"Slack alert sent for {anomaly.get('account_id')}")
        else:
            print(f"No SLACK_WEBHOOK_URL configured. Anomaly: {json.dumps(anomaly)}")

    return {"statusCode": 200}
