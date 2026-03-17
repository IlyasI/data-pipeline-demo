"""
Pipeline monitoring module.

Tracks pipeline execution metrics, detects anomalies, and sends alerts.
In production, this would integrate with Datadog/PagerDuty/Slack. Here
we demonstrate the patterns with structured logging and optional webhook
alerts.

Usage:
    # Typically used via the orchestrator, but can be used standalone:
    from pipeline.monitor import PipelineMonitor, ExecutionRecord

    monitor = PipelineMonitor()
    monitor.record_execution(ExecutionRecord(...))
    monitor.check_anomalies()
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExecutionRecord:
    """Record of a single pipeline stage execution."""

    stage: str
    status: str  # "success" or "failed"
    duration_seconds: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    row_count: int = 0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyDetection:
    """Result of an anomaly check."""

    metric: str
    current_value: float
    expected_range: tuple[float, float]
    is_anomaly: bool
    message: str


class PipelineMonitor:
    """Monitors pipeline execution and detects anomalies.

    Tracks execution history, computes statistics, and sends alerts when
    metrics fall outside expected ranges. Execution history is persisted
    to a local JSON file for trend analysis.

    Attributes:
        history_path: Path to the JSON file storing execution history.
        slack_webhook_url: Optional Slack webhook for alerts.
        alert_cooldown_seconds: Minimum seconds between alerts of the same type.
    """

    def __init__(
        self,
        history_path: Path | None = None,
        slack_webhook_url: str | None = None,
        alert_cooldown_seconds: int = 300,
    ) -> None:
        self.history_path = history_path or Path("data/execution_history.json")
        self.slack_webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self.alert_cooldown_seconds = alert_cooldown_seconds
        self._history: list[dict[str, Any]] = []
        self._last_alert_times: dict[str, float] = {}

        self._load_history()

    def _load_history(self) -> None:
        """Load execution history from disk."""
        if self.history_path.exists():
            try:
                with open(self.history_path, encoding="utf-8") as f:
                    self._history = json.load(f)
                logger.debug("Loaded %d history records from %s", len(self._history), self.history_path)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to load history: %s. Starting fresh.", str(exc))
                self._history = []
        else:
            self._history = []

    def _save_history(self) -> None:
        """Persist execution history to disk."""
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.history_path, "w", encoding="utf-8") as f:
            json.dump(self._history, f, indent=2, default=str)
        logger.debug("Saved %d history records to %s", len(self._history), self.history_path)

    def record_execution(self, record: ExecutionRecord) -> None:
        """Record a pipeline stage execution.

        Args:
            record: Execution record to store.
        """
        record_dict = asdict(record)
        record_dict["timestamp"] = record.timestamp.isoformat()
        self._history.append(record_dict)

        # Keep last 1000 records to prevent unbounded growth
        if len(self._history) > 1000:
            self._history = self._history[-1000:]

        self._save_history()

        logger.info(
            "Recorded execution: stage=%s status=%s duration=%.2fs rows=%d",
            record.stage,
            record.status,
            record.duration_seconds,
            record.row_count,
        )

    def get_stage_history(self, stage: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent execution history for a specific stage.

        Args:
            stage: Stage name to filter by.
            limit: Maximum number of records to return.

        Returns:
            List of execution records, most recent first.
        """
        stage_records = [r for r in self._history if r.get("stage") == stage]
        return stage_records[-limit:]

    def compute_stage_stats(self, stage: str) -> dict[str, float]:
        """Compute execution statistics for a stage.

        Calculates mean and standard deviation of duration and row count
        from the last 20 successful executions. These are used as baselines
        for anomaly detection.

        Args:
            stage: Stage name to compute stats for.

        Returns:
            Dict with avg_duration, std_duration, avg_row_count, std_row_count,
            success_rate.
        """
        history = self.get_stage_history(stage)
        if not history:
            return {
                "avg_duration": 0.0,
                "std_duration": 0.0,
                "avg_row_count": 0.0,
                "std_row_count": 0.0,
                "success_rate": 0.0,
            }

        successful = [r for r in history if r.get("status") == "success"]
        durations = [r["duration_seconds"] for r in successful]
        row_counts = [r.get("row_count", 0) for r in successful]

        def _mean(values: list[float]) -> float:
            return sum(values) / len(values) if values else 0.0

        def _std(values: list[float]) -> float:
            if len(values) < 2:
                return 0.0
            mean = _mean(values)
            variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
            return variance**0.5

        return {
            "avg_duration": _mean(durations),
            "std_duration": _std(durations),
            "avg_row_count": _mean(row_counts),
            "std_row_count": _std(row_counts),
            "success_rate": len(successful) / len(history) if history else 0.0,
        }

    def check_anomalies(self, record: ExecutionRecord) -> list[AnomalyDetection]:
        """Check for anomalies in a new execution record.

        Compares the new record against historical baselines. Flags anomalies
        when duration or row count falls outside 3 standard deviations from
        the mean.

        Args:
            record: New execution record to check.

        Returns:
            List of detected anomalies.
        """
        anomalies: list[AnomalyDetection] = []
        stats = self.compute_stage_stats(record.stage)

        # Duration anomaly: flag if > 3 standard deviations from mean
        if stats["avg_duration"] > 0 and stats["std_duration"] > 0:
            lower = max(0, stats["avg_duration"] - 3 * stats["std_duration"])
            upper = stats["avg_duration"] + 3 * stats["std_duration"]

            if record.duration_seconds < lower or record.duration_seconds > upper:
                anomalies.append(AnomalyDetection(
                    metric="duration",
                    current_value=record.duration_seconds,
                    expected_range=(lower, upper),
                    is_anomaly=True,
                    message=(
                        f"Stage '{record.stage}' duration {record.duration_seconds:.2f}s "
                        f"is outside expected range [{lower:.2f}s, {upper:.2f}s]"
                    ),
                ))

        # Row count anomaly: flag significant drops
        if stats["avg_row_count"] > 0 and record.row_count > 0:
            expected_min = stats["avg_row_count"] * 0.5  # 50% drop threshold
            if record.row_count < expected_min:
                anomalies.append(AnomalyDetection(
                    metric="row_count",
                    current_value=float(record.row_count),
                    expected_range=(expected_min, float("inf")),
                    is_anomaly=True,
                    message=(
                        f"Stage '{record.stage}' produced {record.row_count} rows, "
                        f"expected at least {expected_min:.0f} "
                        f"(avg: {stats['avg_row_count']:.0f})"
                    ),
                ))

        # Log anomalies
        for anomaly in anomalies:
            logger.warning("ANOMALY DETECTED: %s", anomaly.message)

        return anomalies

    def alert(
        self,
        title: str,
        message: str,
        severity: str = "warning",
    ) -> bool:
        """Send an alert notification.

        Supports Slack webhooks and structured logging. Implements cooldown
        to prevent alert storms.

        Args:
            title: Alert title.
            message: Alert body.
            severity: Alert severity ("info", "warning", "error").

        Returns:
            True if alert was sent, False if suppressed by cooldown.
        """
        # Check cooldown
        alert_key = f"{title}:{severity}"
        now = time.monotonic()
        last_alert = self._last_alert_times.get(alert_key, 0)

        if now - last_alert < self.alert_cooldown_seconds:
            logger.debug(
                "Alert suppressed (cooldown): %s (last sent %.0fs ago)",
                title,
                now - last_alert,
            )
            return False

        self._last_alert_times[alert_key] = now

        # Always log the alert
        log_fn = {
            "info": logger.info,
            "warning": logger.warning,
            "error": logger.error,
        }.get(severity, logger.warning)
        log_fn("ALERT [%s]: %s\n%s", severity.upper(), title, message)

        # Send to Slack if configured
        if self.slack_webhook_url:
            self._send_slack_alert(title, message, severity)

        return True

    def _send_slack_alert(self, title: str, message: str, severity: str) -> None:
        """Send alert to Slack webhook.

        Args:
            title: Alert title.
            message: Alert body.
            severity: Alert severity for emoji selection.
        """
        try:
            import requests

            emoji = {"info": "info", "warning": "warning", "error": "rotating_light"}.get(
                severity, "bell"
            )

            payload = {
                "text": f":{emoji}: *{title}*",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f":{emoji}: {title}"},
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"```{message}```"},
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"Severity: *{severity}* | {datetime.now(timezone.utc).isoformat()}",
                            }
                        ],
                    },
                ],
            }

            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            logger.info("Slack alert sent: %s", title)

        except Exception as exc:
            logger.error("Failed to send Slack alert: %s", str(exc))

    def get_dashboard_metrics(self) -> dict[str, Any]:
        """Generate a summary of pipeline health metrics.

        Returns a dict suitable for rendering a monitoring dashboard or
        exporting to a metrics system.

        Returns:
            Dict with per-stage stats, overall success rate, and recent failures.
        """
        stages = set(r.get("stage", "") for r in self._history)

        stage_metrics = {}
        for stage in sorted(stages):
            stats = self.compute_stage_stats(stage)
            recent = self.get_stage_history(stage, limit=5)
            stage_metrics[stage] = {
                **stats,
                "last_run": recent[-1] if recent else None,
                "recent_failures": sum(
                    1 for r in recent if r.get("status") == "failed"
                ),
            }

        total = len(self._history)
        successes = sum(1 for r in self._history if r.get("status") == "success")

        return {
            "overall_success_rate": successes / total if total > 0 else 0.0,
            "total_executions": total,
            "stages": stage_metrics,
            "recent_failures": [
                r for r in self._history[-20:] if r.get("status") == "failed"
            ],
        }
