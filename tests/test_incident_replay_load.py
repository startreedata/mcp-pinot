"""Exercise full-row parity across the former fixed selection limit."""

import json
import sys

from examples.incident_replay import load
import httpx
import pytest


@pytest.mark.parametrize("expected_count", [100001, 100000])
def test_parity_reads_over_100000_rows_and_rejects_a_matching_prefix(
    tmp_path, monkeypatch, expected_count
):
    (tmp_path / "public.json").write_text('{"table":"incident_replay_99"}')
    (tmp_path / "schema.json").write_text(
        '{"metricFieldSpecs":[],"dateTimeFieldSpecs":[]}'
    )
    with (tmp_path / "telemetry.csv").open("w") as stream:
        stream.write("eventId\n")
        for index in range(expected_count):
            stream.write(f"{index:06d}\n")
    native_rows = [[f"{index:06d}"] for index in range(100001)]

    def respond(request):
        limit = int(json.loads(request.content)["sql"].rsplit(" ", 1)[-1])
        return httpx.Response(
            200,
            json={
                "exceptions": [],
                "numServersQueried": 1,
                "numServersResponded": 1,
                "resultTable": {
                    "dataSchema": {"columnNames": ["eventId"]},
                    "rows": native_rows[:limit],
                },
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(respond))
    monkeypatch.setattr(load.httpx, "Client", lambda **_kwargs: client)
    # An incorrect row count fails immediately instead of sleeping for a minute.
    ticks = iter([0, 61])
    monkeypatch.setattr(load.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "parity.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "load.py",
            "--dataset",
            str(tmp_path),
            "--broker",
            "http://127.0.0.1:8000",
            "--controller",
            "http://127.0.0.1:9000",
            "--verify-only",
            "--output",
            str(output),
        ],
    )
    if expected_count < len(native_rows):
        with pytest.raises(RuntimeError, match="visibility deadline"):
            load.main()
        assert not output.exists()
    else:
        load.main()
        receipt = json.loads(output.read_text())
        assert receipt["source_rows"] == receipt["native_rows"] == expected_count
        assert receipt["full_row_parity"] is True
