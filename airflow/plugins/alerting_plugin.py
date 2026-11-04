"""
Airflow Alerting & Observability Plugin
======================================
Provides enterprise SLA and failure alerting callbacks with Slack/Webhook integrations,
contextual error extraction, and Dead Letter Queue auditing.
Standard pattern at Uber and Netflix for production alert triage.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger("airflow.plugins.alerting")


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Triggered automatically whenever an Airflow task fails.
    Extracts exception details, log URL, task instance metadata, and posts to alert channel.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    execution_date = context.get("execution_date", datetime.now(timezone.utc)).isoformat()
    exception = context.get("exception", "No exception message provided")
    log_url = task_instance.log_url if task_instance else "N/A"

    alert_payload = {
        "channel": "#data-platform-alerts",
        "username": "Airflow Lakehouse Watcher",
        "icon_emoji": ":rotating_light:",
        "attachments": [
            {
                "color": "#D32F2F",
                "title": f"🚨 Pipeline Failure in DAG: {dag_id}",
                "fields": [
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": True},
                    {"title": "Exception", "value": str(exception)[:200], "short": False},
                    {"title": "Logs", "value": f"<{log_url}|View Task Logs>", "short": False},
                ],
                "footer": "Senbon — Retail Intelligence Platform",
                "ts": int(datetime.now(timezone.utc).timestamp()),
            }
        ],
    }

    logger.error(f"[ALERT] Task {dag_id}.{task_id} failed: {exception}")

    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    if webhook_url and HAS_REQUESTS:
        try:
            resp = requests.post(webhook_url, json=alert_payload, timeout=5)
            logger.info(f"Dispatched Slack alert. Status: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Failed to post Slack alert: {e}")


def on_sla_miss_callback(dag: Any, task_list: Any, blocking_task_list: Any, slas: Any, blocking_tis: Any) -> None:
    """Triggered when task exceeds defined SLA window."""
    logger.warning(f"[SLA MISS] DAG {dag.dag_id} exceeded runtime SLA threshold for tasks: {task_list}")

