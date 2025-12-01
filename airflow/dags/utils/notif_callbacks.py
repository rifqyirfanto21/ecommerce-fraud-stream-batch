import logging
import requests
from airflow.hooks.base import BaseHook

DISCORD_CONN_ID = "discord_webhook_default"

# ---------- small helpers ----------

def _get_run_display(run_id: str) -> str:
    """
    Format Airflow run_id into a nicer label.
    """
    if "manual__" in run_id:
        # manual__2025-11-30T12:34:56+00:00
        without_prefix = run_id.replace("manual__", "")
        execution_date = without_prefix.split("T")[0]
        execution_time = without_prefix.split("T")[1].split("+")[0][:8]
        return f"Manual Run - {execution_date} {execution_time}"
    else:
        return run_id


def _get_dag_display_name(dag_id: str) -> str:
    """
    Make DAG id more human readable: my_cool_dag -> My Cool Dag
    """
    return dag_id.replace("_", " ").title()


def _send_discord_embed(
    title: str,
    description: str,
    color: int,
    fields: list,
    footer_text: str | None = None,
) -> None:
    """
    Generic helper to send a Discord embed to a webhook.
    `fields` is a list of dicts: {"name": "...", "value": "...", "inline": bool}
    """
    try:
        discord_conn = BaseHook.get_connection(DISCORD_CONN_ID)
        webhook_url = discord_conn.password

        embed = {
            "title": title,
            "description": description,
            "color": color,
            "fields": fields,
        }

        if footer_text:
            embed["footer"] = {"text": footer_text}

        payload = {
            "embeds": [embed]
        }

        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()

    except Exception as e:
        logging.warning(f"Discord alert failed: {e}")


# ---------- 1) Data consistent ----------

def consistent_notif_message(context, **kwargs):
    """
    Sends Discord embed when the data is consistent.
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    dag_display_name = _get_dag_display_name(dag_id)
    run_display = _get_run_display(run_id)

    ingested = kwargs.get("ingested", "N/A")
    loaded = kwargs.get("loaded", "N/A")

    title = "✅ Data Consistency Check Passed"
    description = (
        "All data successfully transferred and validated.\n"
        "No discrepancies detected between staging and target."
    )
    color = 0x2ECC71  # green

    fields = [
        {"name": "DAG", "value": dag_display_name, "inline": False},
        {"name": "Task", "value": task_id, "inline": False},
        {"name": "Run", "value": run_display, "inline": False},
        {"name": "Rows Ingested", "value": str(ingested), "inline": True},
        {"name": "Rows Loaded", "value": str(loaded), "inline": True},
        {"name": "Status", "value": "✅ Data Consistent", "inline": False},
    ]

    footer_text = "Airflow • Consistency Monitor"

    _send_discord_embed(title, description, color, fields, footer_text)
    logging.info(f"Discord alert sent for consistent DAG: {dag_id}")


# ---------- 2) Data inconsistent ----------

def inconsistent_notif_message(context, **kwargs):
    """
    Sends Discord embed when the data is inconsistent.
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    dag_display_name = _get_dag_display_name(dag_id)
    run_display = _get_run_display(run_id)

    ingested = kwargs.get("ingested", "N/A")
    loaded = kwargs.get("loaded", "N/A")

    title = "⚠️ Data Inconsistency Detected"
    description = (
        "Row count mismatch between staging and target.\n"
        "Please investigate the data pipeline immediately."
    )
    color = 0xF1C40F  # yellow / orange

    fields = [
        {"name": "DAG", "value": dag_display_name, "inline": False},
        {"name": "Task", "value": task_id, "inline": False},
        {"name": "Run", "value": run_display, "inline": False},
        {"name": "Rows Ingested", "value": str(ingested), "inline": True},
        {"name": "Rows Loaded", "value": str(loaded), "inline": True},
        {"name": "Status", "value": "❌ Data Inconsistent", "inline": False},
    ]

    footer_text = "Airflow • Consistency Monitor"

    _send_discord_embed(title, description, color, fields, footer_text)
    logging.info(f"Discord alert sent for inconsistent DAG: {dag_id}")


# ---------- 3) DAG success (overall) ----------

def dag_success_notif_message(**context):
    """
    Sends Discord embed when the DAG run is successful.
    To be used as on_success_callback at DAG level.
    """
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    dag_display_name = _get_dag_display_name(dag_id)
    run_display = _get_run_display(run_id)

    title = "🎉 DAG Execution Successful"
    description = "All tasks completed successfully, and data has been processed as expected."
    color = 0x2ECC71  # green

    fields = [
        {"name": "DAG", "value": dag_display_name, "inline": False},
        {"name": "Run", "value": run_display, "inline": False},
        {"name": "Status", "value": "✅ DAG Successful", "inline": False},
    ]

    footer_text = "Airflow • DAG Status"

    _send_discord_embed(title, description, color, fields, footer_text)
    logging.info(f"Discord alert sent for successful DAG: {dag_id}")


# ---------- 4) Task / DAG failure ----------

def failed_notif_message(context):
    """
    Sends Discord embed when a task fails.
    Can be used as on_failure_callback for tasks,
    or at DAG level if bound appropriately.
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    dag_display_name = _get_dag_display_name(dag_id)
    run_display = _get_run_display(run_id)

    exception = context.get("exception")
    error_message = str(exception) if exception else "Unknown Error"

    # Optional: truncate very long error messages to avoid giant Discord messages
    if len(error_message) > 800:
        error_message = error_message[:797] + "..."

    title = "🚨 DAG Execution Failed"
    description = "A task in the DAG has failed. Please investigate and resolve the issue."
    color = 0xE74C3C  # red

    fields = [
        {"name": "DAG", "value": dag_display_name, "inline": False},
        {"name": "Task", "value": task_id, "inline": False},
        {"name": "Run", "value": run_display, "inline": False},
        {"name": "Error", "value": f"```{error_message}```", "inline": False},
    ]

    footer_text = "Airflow • Failure Alert"

    _send_discord_embed(title, description, color, fields, footer_text)
    logging.info(f"Discord alert sent for failed task: {task_id}")