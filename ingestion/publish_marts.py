"""Export the dbt marts from the local DuckDB warehouse to Parquet files.

Runs after `dbt build --target local`. Each mart becomes one Parquet file under
data/marts/, which a host step then syncs to S3 (public-read). The public
dashboard reads those Parquet files directly over HTTPS, so it depends on neither
the VM nor MotherDuck.

    python -m ingestion.publish_marts
"""
from __future__ import annotations

import pathlib

import duckdb

WAREHOUSE = "/app/data/warehouse.duckdb"
OUT = pathlib.Path("/app/data/marts")

# The marts the public dashboard reads.
MARTS = [
    "fct_ingest_summary",
    "fct_dead_letter_reasons",
    "fct_coherence",
    "rec_stream_vs_batch",
    "fct_calibration",
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(WAREHOUSE, read_only=True)
    existing = {
        r[0]
        for r in con.execute(
            "select table_name from information_schema.tables where table_schema='main'"
        ).fetchall()
    }
    for mart in MARTS:
        if mart not in existing:
            print(f"skip {mart} (not built yet)")
            continue
        con.execute(
            f"COPY (SELECT * FROM main.{mart}) TO '{OUT / (mart + '.parquet')}' (FORMAT PARQUET)"
        )
        print(f"exported {mart}")


if __name__ == "__main__":
    main()
