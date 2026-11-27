"""Extract SQL Server extract views to Parquet snapshots in the Databricks landing volume."""
import io
import os
from datetime import datetime, timezone

import mssql_python
import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sdk import WorkspaceClient

SQL_CONN = os.environ.get(
    "SQLSERVER_CONN",
    "Server=localhost;Database=fleet_master;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes",
)
LANDING = "/Volumes/nyc_mobility/bronze/landing/sqlserver"
VIEWS = {"vendor": "ref.v_vendor_extract", "rate_code": "ref.v_rate_code_extract"}


def read_view(cur, view):
    cur.execute(f"SELECT * FROM {view}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})


def main():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    client = WorkspaceClient()
    conn = mssql_python.connect(SQL_CONN)
    cur = conn.cursor()
    results = []
    for name, view in VIEWS.items():
        table = read_view(cur, view)
        table = table.append_column("extracted_at", pa.array([now] * table.num_rows, pa.timestamp("us")))
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        path = f"{LANDING}/{name}/{name}_{stamp}.parquet"
        client.files.upload(path, buf, overwrite=False)
        print(f"{name}: {table.num_rows} rows -> {path}")
        results.append({"source": name, "rows": table.num_rows, "path": path})
    conn.close()
    return results


if __name__ == "__main__":
    main()