"""Exercise owner/state boundaries and actual fixed SQL/evidence contracts."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from threading import Barrier, Event
from typing import Any

from pydantic import ValidationError
import pytest
import sqlglot

from mcp_pinot.incidents import (
    IncidentEvidence,
    IncidentHypothesis,
    IncidentProfile,
    IncidentService,
)
from mcp_pinot.models import QueryExecutionMetadata, QueryExecutionResult


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def profile(**updates: Any) -> IncidentProfile:
    return IncidentProfile(
        **{
            "table": "observability.spans",
            "authorized_principals": ["alice", "bob"],
            "tenant_value": "tenant-a",
            "span_column": "spanId",
            "parent_span_column": "parentSpanId",
        }
        | updates
    )


def result(sql: str, rows: list[dict[str, Any]] | None = None) -> QueryExecutionResult:
    return QueryExecutionResult(
        columns=sqlglot.parse_one(sql, read="trino").named_selects,
        rows=rows or [],
        metadata=QueryExecutionMetadata(
            completeness="complete",
            partial_result=False,
            execution_limit_reached=False,
            servers_queried=1,
            servers_responded=1,
            query_sha256=hashlib.sha256(sql.encode()).hexdigest(),
        ),
    )


class Broker:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, float]] = []
        self.rows: list[dict[str, Any]] = []
        self.error: Exception | None = None
        self.metadata: dict[str, Any] = {}

    def __call__(
        self, sql: str, *, max_rows: int, timeout_seconds: float
    ) -> QueryExecutionResult:
        self.calls.append((sql, max_rows, timeout_seconds))
        if self.error:
            raise self.error
        response = result(sql, self.rows)
        response.metadata = response.metadata.model_copy(update=self.metadata)
        return response


def service(
    p: IncidentProfile | None = None, **kwargs: Any
) -> tuple[IncidentService, Broker, Clock]:
    clock, broker = Clock(), Broker()
    return (
        IncidentService(
            {"checkout": p or profile()},
            broker,
            clock=clock,
            wall_clock=lambda: 10.0,
            **kwargs,
        ),
        broker,
        clock,
    )


def begin(s: IncidentService, **kwargs: Any) -> str:
    return s.begin(
        "checkout", "checkout", 1000, 5000, 9000, principal="alice", **kwargs
    ).run_id


@pytest.mark.parametrize(
    "updates",
    [
        {"table": "x; DROP TABLE y"},
        {"table": "a.b.c"},
        {"service_column": 'service" OR TRUE'},
        {"tenant_value": "\x00"},
        {"authorized_principals": ["*"]},
        {"authorized_principals": ["alice?"]},
        {"authorized_principals": []},
        {"authorized_principals": ["alice", "alice"]},
        {"parent_span_column": None},
        {"max_queries": True},
        {"max_inflight": 0},
        {"max_rows": 1001},
        {"deadline_seconds": -1},
        {"unknown": 1},
    ],
)
def test_profile_rejects_unsafe_or_unbounded_policy(updates: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        profile(**updates)


def test_profile_snapshot_is_not_mutated_by_owner() -> None:
    p = profile()
    s, _, _ = service(p)
    p.authorized_principals.append("mallory")
    with pytest.raises(ValueError, match="unauthorized"):
        s.begin("checkout", "checkout", 1000, 5000, 9000, principal="mallory")


@pytest.mark.parametrize(
    "times",
    [
        (1000, 1000, 9000),
        (1000, 9000, 9000),
        (1000, 5000, 11000),
        (True, 5000, 9000),
        (-1, 5000, 9000),
        (0, 1, 3600001),
    ],
)
def test_rejects_open_unordered_or_oversized_windows(
    times: tuple[int, int, int],
) -> None:
    s, _, _ = service()
    with pytest.raises(ValueError, match=r"window|Windows"):
        s.begin("checkout", "checkout", *times, principal="alice")


def test_principal_and_immutable_citation_binding() -> None:
    s, broker, _ = service()
    run = begin(s)
    with pytest.raises(ValueError, match="unauthorized"):
        s.query(run, "incident", principal="bob")
    evidence = s.query(run, "incident", principal="alice")
    assert evidence.complete
    assert broker.calls[0][1:] == (1001, 55.0)
    digest = hashlib.sha256(
        json.dumps(
            evidence.model_dump(exclude={"sha256"}), sort_keys=True, allow_nan=False
        ).encode()
    ).hexdigest()
    assert evidence.sha256 == digest
    evidence.complete = False
    evidence.rows.append({"forged": "row"})
    evidence.metadata.completeness = "partial"
    with pytest.raises(ValueError, match="Forged"):
        s.finish(run, ["ev_forged"], status="abstained", principal="alice")
    other = begin(s)
    foreign = s.query(other, "baseline", principal="alice")
    with pytest.raises(ValueError, match="foreign"):
        s.finish(run, [foreign.evidence_id], status="abstained", principal="alice")
    finished = s.finish(
        run, [evidence.evidence_id], status="abstained", principal="alice"
    )
    assert finished.citations == {evidence.evidence_id: digest}
    assert not finished.hypothesis_validated and not finished.confirmed_cause
    with pytest.raises(ValueError, match="closed"):
        s.finish(run, [], status="incomplete", reason="done", principal="alice")
    with pytest.raises(ValueError, match="closed"):
        s.query(run, "incident", principal="alice")


def test_fixed_tenant_time_predicates_and_literal_escaping() -> None:
    s, broker, _ = service(profile(tenant_value="a' OR 1=1 --"))
    run = begin(s)
    for kind in ("baseline", "incident", "watermark", "changes"):
        s.query(run, kind, principal="alice")
    candidate = IncidentHypothesis(
        kind="deployment", service="orders' --", version="v1", zone="east"
    )
    s.query(run, "onset", candidate, principal="alice")
    for index, (sql, _, _) in enumerate(broker.calls):
        expression = sqlglot.parse_one(sql, read="trino")
        assert (
            expression.find(sqlglot.exp.Table).sql(dialect="trino")
            == '"observability"."spans"'
        )
        literals = [
            literal.this for literal in expression.find_all(sqlglot.exp.Literal)
        ]
        assert "a' OR 1=1 --" in literals
        assert '"tenant" =' in sql
        assert f'"eventTs" >= {1000 if index == 0 else 5000}' in sql
        assert f'"eventTs" < {5000 if index == 0 else 9000}' in sql
        assert "LIMIT 1001" in sql
    assert "orders' --" in [
        x.this
        for x in sqlglot.parse_one(broker.calls[-1][0]).find_all(sqlglot.exp.Literal)
    ]
    assert 'MIN("eventTs")' in broker.calls[-1][0]
    with pytest.raises(ValueError, match="Candidate"):
        s.query(run, "incident", candidate, principal="alice")
    with pytest.raises(ValueError, match="requires candidate"):
        s.query(run, "onset", principal="alice")


def test_real_span_parentage_fields_are_returned() -> None:
    s, broker, _ = service()
    run = begin(s)
    broker.rows = [
        {
            "traceId": "tr1",
            "spanId": "root",
            "parentSpanId": "",
            "service": "checkout",
            "eventTs": 6000,
            "errorClass": "ok",
        },
        {
            "traceId": "tr1",
            "spanId": "child",
            "parentSpanId": "root",
            "service": "orders",
            "eventTs": 6100,
            "errorClass": "error",
        },
    ]
    evidence = s.get_trace(run, "tr1", principal="alice")
    assert (
        evidence.complete
        and evidence.rows[1]["parentSpanId"] == evidence.rows[0]["spanId"]
    )
    assert "\"traceId\" = 'tr1'" in evidence.sql
    assert "parentService" not in evidence.sql and "JOIN" not in evidence.sql
    missing, _, _ = service(profile(span_column=None, parent_span_column=None))
    with pytest.raises(ValueError, match="actual span"):
        missing.get_trace(begin(missing), "tr1", principal="alice")


@pytest.mark.parametrize(
    "metadata",
    [
        {"completeness": "unknown"},
        {"completeness": "partial"},
        {"partial_result": True},
        {"execution_limit_reached": True},
        {"row_limit_reached": True},
        {"servers_responded": 0},
        {"servers_queried": 0},
        {"unknown_reasons": ["missing stats"]},
        {"query_sha256": "0" * 64},
    ],
)
def test_native_uncertainty_fails_closed(metadata: dict[str, Any]) -> None:
    s, broker, _ = service()
    broker.metadata = metadata
    run = begin(s)
    evidence = s.query(run, "incident", principal="alice")
    assert not evidence.complete and evidence.error
    with pytest.raises(ValueError, match="Incomplete evidence"):
        s.finish(run, [evidence.evidence_id], status="abstained", principal="alice")
    finished = s.finish(
        run,
        [evidence.evidence_id],
        status="incomplete",
        reason="Native evidence uncertain",
        principal="alice",
    )
    assert finished.failed_evidence_ids == [evidence.evidence_id]


def test_failed_attempt_charged_before_retry_and_no_details_leaked() -> None:
    s, broker, _ = service(profile(max_queries=1))
    run = begin(s)
    broker.error = RuntimeError("secret credentials and native SQL")
    evidence = s.query(run, "incident", principal="alice")
    assert not evidence.complete and "secret" not in (evidence.error or "")
    with pytest.raises(ValueError, match="budget"):
        s.query(run, "baseline", principal="alice")
    finished = s.finish(
        run, [], status="incomplete", reason="HTTP failure", principal="alice"
    )
    assert finished.query_count == 1 and len(broker.calls) == 1
    assert finished.failed_evidence_ids == [evidence.evidence_id]


@pytest.mark.parametrize("budget", ["max_response_bytes", "max_total_response_bytes"])
def test_startup_rejects_budgets_that_cannot_retain_bounded_query_failures(
    budget: str,
) -> None:
    p = profile(
        table="T" * 128 + "." + "U" * 128,
        tenant_column="N" * 128,
        service_column="S" * 128,
        time_column="D" * 128,
        event_column="E" * 128,
        error_column="R" * 128,
        version_column="V" * 128,
        zone_column="Z" * 128,
        tenant_value="n" * 256,
        **{budget: 1024},
    )
    with pytest.raises(ValueError, match="byte budgets"):
        service(p)


def test_remaining_byte_budget_rejects_before_native_submission() -> None:
    s, broker, _ = service(profile(max_total_response_bytes=8192))
    run = begin(s)
    for _ in range(16):
        before = len(broker.calls)
        try:
            s.query(run, "incident", principal="alice")
        except ValueError as error:
            assert "retained-evidence budget" in str(error)
            assert before == len(broker.calls)
            break
    else:
        pytest.fail("Expected the retained byte budget to reject further queries.")
    finished = s.finish(
        run, [], status="incomplete", reason="byte budget exhausted", principal="alice"
    )
    assert finished.query_count == len(broker.calls) > 0


def test_concurrent_queries_reserve_room_for_failure_evidence() -> None:
    entered = Barrier(3)

    def query(
        sql: str, *, max_rows: int, timeout_seconds: float
    ) -> QueryExecutionResult:
        entered.wait(timeout=5)
        return result(
            sql,
            [
                {
                    "service": "x" * 6500,
                    "version": "v",
                    "zone": "z",
                    "errorClass": "ok",
                    "count": 1,
                }
            ],
        )

    clock = Clock()
    s = IncidentService(
        {"checkout": profile(max_response_bytes=8192, max_total_response_bytes=8192)},
        query,
        clock=clock,
        wall_clock=lambda: 10.0,
    )
    run = begin(s)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(s.query, run, "incident", principal="alice") for _ in range(2)
        ]
        entered.wait(timeout=5)
        evidence = [future.result(timeout=5) for future in futures]
    assert any(not record.complete for record in evidence)
    assert sum(len(record.model_dump_json().encode()) for record in evidence) <= 8192
    finished = s.finish(
        run,
        [record.evidence_id for record in evidence],
        status="incomplete",
        reason="byte budget exhausted",
        principal="alice",
    )
    assert finished.query_count == 2
    assert set(finished.failed_evidence_ids) == {
        record.evidence_id for record in evidence if not record.complete
    }


def test_expiry_restart_and_capacity_fail_closed() -> None:
    s, _, clock = service(ttl_seconds=10, max_runs=1)
    run = begin(s)
    with pytest.raises(ValueError, match="capacity"):
        begin(s)
    clock.now += 10
    with pytest.raises(ValueError, match="expired"):
        s.finish(run, [], status="incomplete", reason="expired", principal="alice")
    assert begin(s) != run
    restarted, _, _ = service()
    with pytest.raises(ValueError, match="Unknown"):
        restarted.query(run, "incident", principal="alice")


@pytest.mark.parametrize("closed", [False, True])
def test_finished_or_deadline_expired_runs_release_capacity(closed: bool) -> None:
    s, _, clock = service(max_runs=1)
    run = begin(s)
    if closed:
        s.finish(run, [], status="incomplete", reason="finished", principal="alice")
    else:
        clock.now += 55
    assert begin(s) != run


@pytest.mark.parametrize("ttl", [10, 3600])
def test_late_completion_is_failed_and_remains_inflight_until_return(ttl: int) -> None:
    clock = Clock()
    entered, release = Event(), Event()

    def query(
        sql: str, *, max_rows: int, timeout_seconds: float
    ) -> QueryExecutionResult:
        entered.set()
        assert release.wait(timeout=5)
        return result(sql)

    s = IncidentService(
        {"checkout": profile(max_inflight=1)},
        query,
        clock=clock,
        wall_clock=lambda: 10.0,
        ttl_seconds=ttl,
        max_runs=1,
    )
    run = begin(s)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(s.query, run, "incident", principal="alice")
        try:
            assert entered.wait(timeout=5)
            with pytest.raises(ValueError, match="Pending"):
                s.finish(
                    run, [], status="incomplete", reason="pending", principal="alice"
                )
            with pytest.raises(ValueError, match="admission"):
                s.query(run, "baseline", principal="alice")
            clock.now += 55
            with pytest.raises(ValueError, match="capacity"):
                begin(s)
        finally:
            release.set()
        evidence = future.result(timeout=5)
    assert not evidence.complete and "deadline" in (evidence.error or "")
    assert begin(s) != run


@pytest.mark.parametrize(
    "rows",
    [
        [{"service": "x"}] * 3,
        [
            {
                "service": "x",
                "version": "v",
                "zone": "z",
                "errorClass": "ok",
                "count": float("nan"),
            }
        ],
        [{"wrong_schema": "x"}],
    ],
)
def test_row_bound_schema_and_nonfinite_results(rows: list[dict[str, Any]]) -> None:
    s, broker, _ = service(profile(max_rows=2))
    broker.rows = rows
    evidence = s.query(begin(s), "incident", principal="alice")
    assert not evidence.complete and not evidence.rows


def test_retained_row_and_byte_budget() -> None:
    s, broker, _ = service(profile(max_total_rows=1))
    broker.rows = [
        {"service": "x", "version": "v", "zone": "z", "errorClass": "ok", "count": 1}
    ]
    run = begin(s)
    assert s.query(run, "baseline", principal="alice").complete
    with pytest.raises(ValueError, match="retained-evidence budget"):
        s.query(run, "incident", principal="alice")
    bounded, oversized, _ = service(profile(max_response_bytes=8192))
    oversized.rows = [
        {
            "service": "x" * 8192,
            "version": "v",
            "zone": "z",
            "errorClass": "ok",
            "count": 1,
        }
    ]
    evidence = bounded.query(begin(bounded), "incident", principal="alice")
    assert not evidence.complete and len(evidence.model_dump_json().encode()) <= 8192


def test_finish_foundation_never_semantically_validates_hypothesis() -> None:
    s, _, _ = service()
    run = begin(s)
    evidence = s.query(run, "incident", principal="alice")
    hypothesis = IncidentHypothesis(
        kind="deployment", service="unobserved", version="v99"
    )
    finished = s.finish(run, [evidence.evidence_id], hypothesis, principal="alice")
    assert finished.hypothesis == hypothesis
    assert not finished.hypothesis_validated and not finished.confirmed_cause
    fresh = begin(s)
    for kwargs in (
        {"status": "incomplete"},
        {"status": "proposed"},
        {"status": "abstained", "hypothesis": hypothesis},
        {"status": "incomplete", "reason": ""},
        {"status": "incomplete", "reason": "x" * 4097},
        {"status": "incomplete", "reason": "unknown", "citations": ["x", "x"]},
    ):
        args = {"citations": [], **kwargs}
        with pytest.raises(ValueError):
            s.finish(fresh, principal="alice", **args)


@pytest.mark.parametrize("ttl,expired_at", [(3600, 156.0), (10, 111.0)])
def test_deadline_covers_final_evidence_serialization(
    monkeypatch: pytest.MonkeyPatch, ttl: int, expired_at: float
) -> None:
    s, _, clock = service(ttl_seconds=ttl)
    run = begin(s)
    serialize = IncidentEvidence.model_dump

    def slow_serialize(self: IncidentEvidence, *args: Any, **kwargs: Any) -> Any:
        rendered = serialize(self, *args, **kwargs)
        clock.now = expired_at
        return rendered

    # Keep the actual JSON/hash work; only advance the injected clock during it.
    monkeypatch.setattr(IncidentEvidence, "model_dump", slow_serialize)
    evidence = s.query(run, "incident", principal="alice")
    assert not evidence.complete and "deadline" in (evidence.error or "")
    with pytest.raises(ValueError, match="expired"):
        s.finish(run, [evidence.evidence_id], status="abstained", principal="alice")


@pytest.mark.parametrize("ttl,expired_at", [(3600, 156.0), (10, 111.0)])
def test_deadline_covers_finish_evidence_integrity_scan(
    monkeypatch: pytest.MonkeyPatch, ttl: int, expired_at: float
) -> None:
    s, _, clock = service(ttl_seconds=ttl)
    run = begin(s)
    evidence = s.query(run, "incident", principal="alice")
    serialize = IncidentEvidence.model_dump

    def slow_serialize(self: IncidentEvidence, *args: Any, **kwargs: Any) -> Any:
        rendered = serialize(self, *args, **kwargs)
        clock.now = expired_at
        return rendered

    with monkeypatch.context() as context:
        context.setattr(IncidentEvidence, "model_dump", slow_serialize)
        with pytest.raises(ValueError, match="expired"):
            s.finish(run, [evidence.evidence_id], status="abstained", principal="alice")
    # A rejected finish did not transition to closed; no private state inspection.
    clock.now = 100.0
    assert s.finish(run, [evidence.evidence_id], status="abstained", principal="alice")


@pytest.mark.parametrize(
    "details",
    [
        {"execution_limit_flags": {"maxRowsInWindowReached": True}},
        {"execution_limit_flags": {"maxRowsInJoinReached": True}},
        {"early_termination_reasons": ["DISTINCT_MAX_EXECUTION_TIME"]},
    ],
)
def test_detailed_native_limits_override_contradictory_aggregate(
    details: dict[str, Any],
) -> None:
    s, broker, _ = service()
    broker.metadata = {
        "completeness": "complete",
        "execution_limit_reached": False,
        **details,
    }
    evidence = s.query(begin(s), "incident", principal="alice")
    assert not evidence.complete and evidence.error
