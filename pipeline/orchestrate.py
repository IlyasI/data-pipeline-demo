"""
Pipeline orchestrator.

Coordinates the full ELT pipeline: extract, load, transform, quality check.
Provides retry logic, structured logging, execution timing, and graceful
error handling for each stage.

In production, this would be replaced by Airflow/Dagster/Prefect, but this
demonstrates the orchestration patterns those tools implement under the hood.

Usage:
    python -m pipeline.orchestrate
    python -m pipeline.orchestrate --stages extract load
    python -m pipeline.orchestrate --dry-run
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

from pipeline.extract import extract_all
from pipeline.load import load_all
from pipeline.monitor import PipelineMonitor, ExecutionRecord
from pipeline.quality import run_all_checks, QualityResult

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """Execution status for a pipeline stage."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StageResult:
    """Result of executing a single pipeline stage."""

    name: str
    status: StageStatus
    duration_seconds: float
    started_at: datetime
    error: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class PipelineRun:
    """Metadata for a complete pipeline run."""

    run_id: str
    started_at: datetime
    stages: list[StageResult] = field(default_factory=list)
    completed_at: datetime | None = None

    @property
    def success(self) -> bool:
        return all(
            s.status in (StageStatus.SUCCESS, StageStatus.SKIPPED)
            for s in self.stages
        )

    @property
    def duration_seconds(self) -> float:
        if self.completed_at is None:
            return 0.0
        return (self.completed_at - self.started_at).total_seconds()

    def summary(self) -> str:
        lines = [
            f"Pipeline Run: {self.run_id}",
            f"Status: {'SUCCESS' if self.success else 'FAILED'}",
            f"Duration: {self.duration_seconds:.2f}s",
            f"Stages:",
        ]
        for stage in self.stages:
            status_icon = {
                StageStatus.SUCCESS: "[OK]",
                StageStatus.FAILED: "[FAIL]",
                StageStatus.SKIPPED: "[SKIP]",
            }.get(stage.status, "[??]")
            lines.append(f"  {status_icon} {stage.name} ({stage.duration_seconds:.2f}s)")
            if stage.error:
                lines.append(f"       Error: {stage.error}")
        return "\n".join(lines)


class PipelineOrchestrator:
    """Orchestrates the full ELT pipeline with retry and monitoring.

    Runs stages sequentially: extract -> load -> transform -> quality.
    Each stage can be retried independently. Failures in one stage prevent
    subsequent stages from running (fail-fast).

    Attributes:
        data_dir: Path to raw data directory.
        warehouse: Warehouse backend name.
        monitor: Pipeline monitor instance for tracking execution.
        max_retries: Maximum retries per stage.
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        warehouse: str = "duckdb",
        max_retries: int = 2,
        monitor: PipelineMonitor | None = None,
    ) -> None:
        self.data_dir = data_dir or Path("data/raw")
        self.warehouse = warehouse
        self.max_retries = max_retries
        self.monitor = monitor or PipelineMonitor()

    def _generate_run_id(self) -> str:
        """Generate a unique run ID based on timestamp."""
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _run_stage_with_retry(
        self,
        name: str,
        func: Callable[[], dict[str, object]],
    ) -> StageResult:
        """Execute a pipeline stage with retry logic.

        Uses exponential backoff between retries. Each attempt is logged
        and tracked by the monitor.

        Args:
            name: Human-readable stage name.
            func: Callable that executes the stage logic. Should return
                  a dict of metadata on success, or raise on failure.

        Returns:
            StageResult with execution metadata.
        """
        last_error: str | None = None

        for attempt in range(self.max_retries + 1):
            started_at = datetime.now(timezone.utc)
            start_time = time.monotonic()

            try:
                if attempt > 0:
                    wait_time = 2**attempt
                    logger.info(
                        "Retrying stage '%s' (attempt %d/%d) after %ds...",
                        name, attempt + 1, self.max_retries + 1, wait_time,
                    )
                    time.sleep(wait_time)

                logger.info("Starting stage: %s", name)
                metadata = func()
                duration = time.monotonic() - start_time

                logger.info("Stage '%s' completed in %.2fs", name, duration)

                self.monitor.record_execution(ExecutionRecord(
                    stage=name,
                    status="success",
                    duration_seconds=duration,
                    timestamp=started_at,
                    row_count=metadata.get("row_count", 0),  # type: ignore[arg-type]
                ))

                return StageResult(
                    name=name,
                    status=StageStatus.SUCCESS,
                    duration_seconds=duration,
                    started_at=started_at,
                    metadata=metadata,
                )

            except Exception as exc:
                duration = time.monotonic() - start_time
                last_error = str(exc)
                logger.error(
                    "Stage '%s' failed (attempt %d/%d): %s",
                    name, attempt + 1, self.max_retries + 1, last_error,
                )

                self.monitor.record_execution(ExecutionRecord(
                    stage=name,
                    status="failed",
                    duration_seconds=duration,
                    timestamp=started_at,
                    error=last_error,
                ))

        return StageResult(
            name=name,
            status=StageStatus.FAILED,
            duration_seconds=duration,
            started_at=started_at,
            error=f"Failed after {self.max_retries + 1} attempts: {last_error}",
        )

    def _stage_extract(self) -> dict[str, object]:
        """Execute the extraction stage."""
        results = extract_all(output_dir=self.data_dir)
        failed = [r for r in results if not r.success]
        if failed:
            raise RuntimeError(
                f"Extraction failed for: {', '.join(r.source_name for r in failed)}"
            )
        return {
            "sources_extracted": len(results),
            "row_count": sum(r.row_count for r in results),
            "total_bytes": sum(r.byte_count for r in results),
        }

    def _stage_load(self) -> dict[str, object]:
        """Execute the loading stage."""
        results = load_all(data_dir=self.data_dir, warehouse=self.warehouse)
        failed = [r for r in results if not r.success]
        if failed:
            raise RuntimeError(
                f"Load failed for: {', '.join(r.source_name for r in failed)}"
            )
        return {
            "tables_loaded": len(results),
            "row_count": sum(r.row_count for r in results),
        }

    def _stage_transform(self) -> dict[str, object]:
        """Execute dbt transformation.

        Runs dbt run to build all models. In a real pipeline, this would
        also run dbt test after the build.
        """
        logger.info("Running dbt transformations...")

        # Check if dbt is available
        try:
            result = subprocess.run(
                ["dbt", "debug", "--no-version-check"],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                logger.warning("dbt debug failed, skipping transform: %s", result.stderr)
                raise RuntimeError(
                    "dbt is not properly configured. Skipping transform stage. "
                    "Run 'dbt debug' to diagnose."
                )
        except FileNotFoundError:
            raise RuntimeError(
                "dbt is not installed. Install with: pip install dbt-core dbt-duckdb"
            )

        result = subprocess.run(
            ["dbt", "run"],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            raise RuntimeError(f"dbt run failed:\n{result.stdout}\n{result.stderr}")

        return {"dbt_output": result.stdout[-500:] if result.stdout else ""}

    def _stage_quality(self) -> dict[str, object]:
        """Execute data quality checks."""
        results = run_all_checks(data_dir=self.data_dir)
        failed = [r for r in results if not r.passed]

        return {
            "total_checks": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": len(failed),
            "failed_checks": [r.check_name for r in failed],
            "row_count": 0,
        }

    def run(
        self,
        stages: list[str] | None = None,
        dry_run: bool = False,
    ) -> PipelineRun:
        """Execute the full pipeline.

        Args:
            stages: Optional list of stages to run. Defaults to all stages.
                    Valid stages: extract, load, transform, quality.
            dry_run: If True, log what would happen without executing.

        Returns:
            PipelineRun with results for all stages.
        """
        all_stages: dict[str, Callable[[], dict[str, object]]] = {
            "extract": self._stage_extract,
            "load": self._stage_load,
            "transform": self._stage_transform,
            "quality": self._stage_quality,
        }

        selected = stages or list(all_stages.keys())
        run_id = self._generate_run_id()

        pipeline_run = PipelineRun(
            run_id=run_id,
            started_at=datetime.now(timezone.utc),
        )

        logger.info("=" * 60)
        logger.info("Pipeline run %s starting", run_id)
        logger.info("Stages: %s", ", ".join(selected))
        logger.info("Warehouse: %s", self.warehouse)
        logger.info("=" * 60)

        if dry_run:
            logger.info("[DRY RUN] Would execute stages: %s", ", ".join(selected))
            pipeline_run.completed_at = datetime.now(timezone.utc)
            return pipeline_run

        for stage_name in selected:
            if stage_name not in all_stages:
                logger.warning("Unknown stage '%s', skipping", stage_name)
                continue

            stage_func = all_stages[stage_name]
            result = self._run_stage_with_retry(stage_name, stage_func)
            pipeline_run.stages.append(result)

            # Fail fast: don't run subsequent stages if this one failed
            if result.status == StageStatus.FAILED:
                logger.error(
                    "Stage '%s' failed. Aborting remaining stages.", stage_name
                )
                # Mark remaining stages as skipped
                remaining = selected[selected.index(stage_name) + 1 :]
                for skipped_name in remaining:
                    pipeline_run.stages.append(
                        StageResult(
                            name=skipped_name,
                            status=StageStatus.SKIPPED,
                            duration_seconds=0.0,
                            started_at=datetime.now(timezone.utc),
                        )
                    )
                break

        pipeline_run.completed_at = datetime.now(timezone.utc)

        # Report results
        logger.info("=" * 60)
        logger.info(pipeline_run.summary())
        logger.info("=" * 60)

        # Send alert if pipeline failed
        if not pipeline_run.success:
            self.monitor.alert(
                f"Pipeline {run_id} FAILED",
                pipeline_run.summary(),
                severity="error",
            )

        return pipeline_run


def main() -> None:
    """CLI entrypoint for pipeline orchestration."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the ELT pipeline")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["extract", "load", "transform", "quality"],
        help="Stages to run (default: all)",
    )
    parser.add_argument(
        "--warehouse",
        choices=["duckdb", "bigquery"],
        default="duckdb",
        help="Warehouse backend (default: duckdb)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max retries per stage (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without executing",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    orchestrator = PipelineOrchestrator(
        warehouse=args.warehouse,
        max_retries=args.max_retries,
    )

    pipeline_run = orchestrator.run(stages=args.stages, dry_run=args.dry_run)

    if not pipeline_run.success:
        sys.exit(1)


if __name__ == "__main__":
    main()
