"""Load fresh fixtures into an owned loopback Pinot, or verify full-row parity."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import time

from common import loopback_url
import httpx


def canonical(rows: list[dict]) -> str:
    return hashlib.sha256(
        json.dumps(
            sorted(rows, key=lambda row: row["eventId"]),
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--controller", required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    controller, broker = map(loopback_url, (args.controller, args.broker))
    table = json.loads((args.dataset / "public.json").read_text())["table"]
    if not re.fullmatch(r"incident_replay_[0-9]+", table):
        parser.error("Only generated incident_replay_<seed> tables may be loaded.")
    schema = json.loads((args.dataset / "schema.json").read_text())
    numeric = {
        spec["name"]
        for group in ("metricFieldSpecs", "dateTimeFieldSpecs")
        for spec in schema[group]
    }
    with (args.dataset / "telemetry.csv").open(newline="") as stream:
        expected = [
            {key: int(value) if key in numeric else value for key, value in row.items()}
            for row in csv.DictReader(stream)
        ]
    with httpx.Client(timeout=90, follow_redirects=False) as client:
        if not args.verify_only:
            response = client.get(controller + "/tables")
            response.raise_for_status()
            if table in response.json()["tables"]:
                raise ValueError(
                    "Fixture table already exists; refusing duplicate load."
                )
            response = client.get(controller + "/schemas")
            response.raise_for_status()
            if schema["schemaName"] in response.json():
                raise ValueError("Fixture schema already exists; refusing overwrite.")
            for endpoint, payload in (
                ("/schemas", schema),
                ("/tables", json.loads((args.dataset / "table.json").read_text())),
            ):
                client.post(controller + endpoint, json=payload).raise_for_status()
            with (args.dataset / "telemetry.csv").open("rb") as stream:
                client.post(
                    controller + "/ingestFromFile",
                    params={
                        "tableNameWithType": table + "_OFFLINE",
                        "batchConfigMapStr": json.dumps(
                            {
                                "inputFormat": "csv",
                                "recordReader.prop.multiValueDelimiterEnabled": "false",
                            }
                        ),
                    },
                    files={"file": (table + "_data.csv", stream, "text/csv")},
                ).raise_for_status()
        deadline = time.monotonic() + 60
        while True:
            response = client.post(
                broker + "/query/sql",
                json={"sql": f'SELECT * FROM "{table}" LIMIT 100000'},  # noqa: S608
            )
            response.raise_for_status()
            native = response.json()
            result = native.get("resultTable", {})
            rows = result.get("rows", [])
            if not native.get("exceptions") and len(rows) == len(expected):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Fixture visibility deadline exceeded.")
            time.sleep(1)
    columns = result["dataSchema"]["columnNames"]
    actual = [dict(zip(columns, row, strict=True)) for row in rows]
    source_hash, native_hash = canonical(expected), canonical(actual)
    source_by_id = {row["eventId"]: row for row in expected}
    differences = [
        {
            "event_id": row["eventId"],
            "fields": {
                key: {
                    "source": source_by_id.get(row["eventId"], {}).get(key),
                    "native": value,
                }
                for key, value in row.items()
                if source_by_id.get(row["eventId"], {}).get(key) != value
            },
        }
        for row in actual
        if source_by_id.get(row["eventId"]) != row
    ]
    receipt = {
        "table": table,
        "source_rows": len(expected),
        "native_rows": len(actual),
        "source_sha256": source_hash,
        "native_sha256": native_hash,
        "full_row_parity": source_hash == native_hash,
        "servers_queried": native.get("numServersQueried"),
        "servers_responded": native.get("numServersResponded"),
        "mismatched_rows": len(differences),
        "first_mismatches": differences[:5],
    }
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    if not receipt["full_row_parity"]:
        raise RuntimeError("Native telemetry differs from the source fixture.")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
