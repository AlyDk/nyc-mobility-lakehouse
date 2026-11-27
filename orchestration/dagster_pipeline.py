"""Dagster: extract on-prem SQL Server master data, then trigger the Databricks lakehouse job."""
import sys
from datetime import timedelta
from pathlib import Path

from dagster import (Definitions, MaterializeResult, MetadataValue, ScheduleDefinition,
                     asset, define_asset_job)
from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import extract_sqlserver  # noqa: E402

JOB_SUFFIX = "nyc_mobility_pipeline"


@asset(group_name="on_prem", description="Snapshots of SQL Server extract views uploaded to the landing volume.")
def sqlserver_snapshots():
    results = extract_sqlserver.main()
    return MaterializeResult(metadata={r["source"]: MetadataValue.text(f'{r["rows"]} rows -> {r["path"]}') for r in results})


@asset(deps=[sqlserver_snapshots], group_name="databricks",
       description="Runs the Databricks job: bronze, silver (incl. vendor SCD2), dbt gold.")
def lakehouse_refresh():
    w = WorkspaceClient()
    job = next(j for j in w.jobs.list() if j.settings.name.endswith(JOB_SUFFIX))
    run = w.jobs.run_now(job_id=job.job_id).result(timeout=timedelta(minutes=30))
    result = run.state.result_state.value
    if result != "SUCCESS":
        raise RuntimeError(f"Databricks job ended with {result}: {run.run_page_url}")
    return MaterializeResult(metadata={"result": MetadataValue.text(result),
                                       "run_url": MetadataValue.url(run.run_page_url)})


refresh_job = define_asset_job("refresh_lakehouse", selection="*")
weekly = ScheduleDefinition(job=refresh_job, cron_schedule="0 6 * * 1", execution_timezone="Asia/Riyadh")

defs = Definitions(assets=[sqlserver_snapshots, lakehouse_refresh], jobs=[refresh_job], schedules=[weekly])