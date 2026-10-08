"""Incident-scoped evidence assembly, not a causal or RCA semantic validator.

State is bounded and process-local. Restart loses runs; multi-process deployments
require sticky routing or a shared state implementation. A timed-out HTTP caller
does not prove native cancellation: in-flight work remains counted until it returns.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
import hashlib
import json
import re
import secrets
from threading import Lock
import time
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator
import sqlglot
from sqlglot import exp

from .models import QueryExecutionMetadata, QueryExecutionResult

QueryKind = Literal["baseline", "incident", "watermark", "changes", "onset"]
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_BUDGET_ERROR = "Retained evidence row/byte budget exceeded."


def _value(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 256
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError(f"{name} requires a bounded nonempty string.")
    return value


class IncidentProfile(BaseModel):
    """Server-owned table/tenant authorization and bounded investigation policy."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    table: str
    authorized_principals: list[str] = Field(min_length=1, max_length=128)
    tenant_value: str
    tenant_column: str = "tenant"
    service_column: str = "service"
    time_column: str = "eventTs"
    event_column: str = "eventType"
    error_column: str = "errorClass"
    version_column: str = "version"
    zone_column: str = "zone"
    trace_column: str = "traceId"
    event_id_column: str = "eventId"
    span_column: str | None = None
    parent_span_column: str | None = None
    max_queries: int = Field(default=32, ge=1, le=128)
    max_inflight: int = Field(default=4, ge=1, le=16)
    max_rows: int = Field(default=1000, ge=1, le=1000)
    max_response_bytes: int = Field(default=1048576, ge=1024, le=1048576)
    max_total_rows: int = Field(default=10000, ge=1, le=100000)
    max_total_response_bytes: int = Field(default=4194304, ge=1024, le=67108864)
    deadline_seconds: int = Field(default=55, ge=1, le=300)
    max_window_ms: int = Field(default=3600000, ge=1, le=86400000)

    @model_validator(mode="after")
    def validate_policy(self) -> "IncidentProfile":
        if (
            not all(_IDENTIFIER.fullmatch(part) for part in self.table.split("."))
            or len(self.table.split(".")) > 2
        ):
            raise ValueError("table requires one or two validated identifiers.")
        for name, value in self.model_dump().items():
            if (
                name.endswith("_column")
                and value is not None
                and not _IDENTIFIER.fullmatch(value)
            ):
                raise ValueError(f"Invalid {name} identifier.")
        _value(self.tenant_value, "tenant_value")
        for principal in self.authorized_principals:
            _value(principal, "authorized principal")
            if any(char in principal for char in "*?[]"):
                raise ValueError("Principal wildcards are not allowed.")
        if len(set(self.authorized_principals)) != len(self.authorized_principals):
            raise ValueError("Authorized principals must be unique.")
        if (self.span_column is None) != (self.parent_span_column is None):
            raise ValueError("Configure both span and parent-span columns together.")
        return self


class IncidentHypothesis(BaseModel):
    """Unverified association supplied by a caller, never confirmed as a cause."""

    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["deployment", "traffic_switch", "experiment", "environment"]
    service: str | None = None
    version: str | None = None
    zone: str | None = None

    @model_validator(mode="after")
    def validate_values(self) -> "IncidentHypothesis":
        for name in ("service", "version", "zone"):
            value = getattr(self, name)
            if value is not None:
                _value(value, name)
        return self


class IncidentRun(BaseModel):
    run_id: str
    profile_id: str
    service: str
    baseline_start_ms: int
    start_ms: int
    end_ms: int
    deadline_seconds: int
    max_queries: int
    scope: str = "single_process_incident_evidence_assembly"


class IncidentEvidence(BaseModel):
    evidence_id: str
    run_id: str
    kind: str
    sql: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    metadata: QueryExecutionMetadata = Field(default_factory=QueryExecutionMetadata)
    complete: bool = False
    error: str | None = None
    sha256: str = ""
    dataset_coverage_attested: bool = False


class IncidentFinish(BaseModel):
    run_id: str
    status: Literal["proposed", "abstained", "incomplete"]
    hypothesis: IncidentHypothesis | None
    citations: dict[str, str]
    reason: str | None
    query_count: int
    failed_evidence_ids: list[str]
    hypothesis_validated: bool = False
    confirmed_cause: bool = False
    dataset_coverage_attested: bool = False
    scope: str = "evidence_assembly_without_rca_semantic_validation"


class ExecuteQuery(Protocol):
    def __call__(
        self, sql: str, *, max_rows: int, timeout_seconds: float
    ) -> QueryExecutionResult: ...


@dataclass
class _Run:
    public: IncidentRun
    owner: str
    created: float
    deadline: float
    profile: IncidentProfile
    queries: int = 0
    inflight: int = 0
    retained_rows: int = 0
    retained_bytes: int = 0
    reserved_failure_bytes: int = 0
    closed: bool = False
    evidence: dict[str, str] = field(default_factory=dict)


class IncidentService:
    """Bounded, owner-bound runs; evidence returned to callers is always a copy."""

    def __init__(
        self,
        profiles: dict[str, IncidentProfile],
        execute_query: ExecuteQuery,
        *,
        ttl_seconds: int = 3600,
        max_runs: int = 1000,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 86400:
            raise ValueError("ttl_seconds requires 1..86400.")
        if type(max_runs) is not int or not 1 <= max_runs <= 1000:
            raise ValueError("max_runs requires 1..1000.")
        if not profiles or len(profiles) > 128:
            raise ValueError("Configure 1..128 incident profiles.")
        self._profiles = {}
        for key, profile in profiles.items():
            _value(key, "profile ID")
            self._profiles[key] = IncidentProfile.model_validate(profile.model_dump())
        self._execute_query = execute_query
        self._ttl, self._max_runs = ttl_seconds, max_runs
        self._clock, self._wall_clock = clock, wall_clock
        self._lock = Lock()
        self._runs: dict[str, _Run] = {}
        bounded_value = "\U0010ffff" * 256
        candidate = IncidentHypothesis(
            kind="deployment",
            service=bounded_value,
            version=bounded_value,
            zone=bounded_value,
        )
        for key, profile in self._profiles.items():
            run = _Run(
                IncidentRun(
                    run_id="inc_" + "0" * 32,
                    profile_id=key,
                    service="service",
                    baseline_start_ms=9223372036854775805,
                    start_ms=9223372036854775806,
                    end_ms=9223372036854775807,
                    deadline_seconds=profile.deadline_seconds,
                    max_queries=profile.max_queries,
                ),
                "",
                0,
                0,
                profile,
            )
            kinds = ["baseline", "incident", "watermark", "changes", "onset"]
            if profile.span_column is not None:
                kinds.append("trace")
            for kind in kinds:
                sql = self._sql(run, kind, candidate, bounded_value)
                if self._failure_bytes(sql, kind) > min(
                    profile.max_response_bytes, profile.max_total_response_bytes
                ):
                    raise ValueError(
                        "Incident profile byte budgets cannot retain "
                        "bounded query failures."
                    )

    @staticmethod
    def _failure_bytes(sql: str, kind: str) -> int:
        """Reserve the largest bounded failure, including SQL and its digest."""
        return len(
            IncidentEvidence(
                evidence_id="ev_" + "0" * 32,
                run_id="inc_" + "0" * 32,
                kind=kind,
                sql=sql,
                error="Query execution failed: " + "\x00" * 64,
                sha256="0" * 64,
            )
            .model_dump_json()
            .encode()
        )

    @staticmethod
    def _snapshot(
        evidence: IncidentEvidence, byte_limit: int
    ) -> tuple[str, IncidentEvidence, int]:
        for trim in (None, "rows", "metadata"):
            if trim is not None:
                evidence.complete, evidence.error = False, _BUDGET_ERROR
                evidence.rows = []
            if trim == "metadata":
                evidence.columns, evidence.metadata = [], QueryExecutionMetadata()
            raw = json.dumps(
                evidence.model_dump(exclude={"sha256"}), sort_keys=True, allow_nan=False
            ).encode()
            evidence.sha256 = hashlib.sha256(raw).hexdigest()
            stored = evidence.model_dump_json()
            delivery = IncidentEvidence.model_validate_json(stored)
            stored_bytes = len(stored.encode())
            if stored_bytes <= byte_limit:
                return stored, delivery, stored_bytes
        raise ValueError("Investigation retained-evidence budget exhausted.")

    def _get(self, run_id: str, principal: str) -> _Run:
        run = self._runs.get(run_id)
        if run is None or run.owner != principal:
            raise ValueError("Unknown or unauthorized investigation.")
        now = self._clock()
        if now >= run.deadline or now >= run.created + self._ttl:
            raise ValueError("Investigation expired.")
        if run.closed:
            raise ValueError("Investigation already closed.")
        return run

    def begin(
        self,
        profile_id: str,
        service: str,
        baseline_start_ms: int,
        start_ms: int,
        end_ms: int,
        *,
        principal: str,
    ) -> IncidentRun:
        profile = self._profiles.get(profile_id)
        if profile is None or principal not in profile.authorized_principals:
            raise ValueError("Unknown or unauthorized incident profile.")
        _value(service, "service")
        for value in (baseline_start_ms, start_ms, end_ms):
            if type(value) is not int or not 0 <= value <= 9223372036854775807:
                raise ValueError("Windows require nonnegative integer milliseconds.")
        if not baseline_start_ms < start_ms < end_ms <= int(self._wall_clock() * 1000):
            raise ValueError("Require closed baseline and incident windows.")
        if end_ms - baseline_start_ms > profile.max_window_ms:
            raise ValueError("Investigation window exceeds its bound.")
        public = IncidentRun(
            run_id="inc_" + secrets.token_hex(16),
            profile_id=profile_id,
            service=service,
            baseline_start_ms=baseline_start_ms,
            start_ms=start_ms,
            end_ms=end_ms,
            deadline_seconds=profile.deadline_seconds,
            max_queries=profile.max_queries,
        )
        with self._lock:
            now = self._clock()
            self._runs = {
                key: run
                for key, run in self._runs.items()
                if run.inflight
                or (not run.closed and now < min(run.deadline, run.created + self._ttl))
            }
            if len(self._runs) >= self._max_runs:
                raise ValueError("Investigation capacity reached.")
            self._runs[public.run_id] = _Run(
                public.model_copy(deep=True),
                principal,
                now,
                now + profile.deadline_seconds,
                profile,
            )
        return public

    @staticmethod
    def _column(name: str) -> str:
        return exp.column(name, quoted=True).sql(dialect="trino")

    @staticmethod
    def _literal(value: str) -> str:
        return exp.Literal.string(value).sql(dialect="trino")

    def _sql(
        self,
        run: _Run,
        kind: str,
        candidate: IncidentHypothesis | None = None,
        trace_id: str | None = None,
    ) -> str:
        p, alert = run.profile, run.public
        c, literal = self._column, self._literal
        start = alert.baseline_start_ms if kind == "baseline" else alert.start_ms
        end = alert.start_ms if kind == "baseline" else alert.end_ms
        predicates = [
            f"{c(p.tenant_column)} = {literal(p.tenant_value)}",
            f"{c(p.time_column)} >= {start}",
            f"{c(p.time_column)} < {end}",
        ]
        suffix = ""
        if kind in ("baseline", "incident"):
            dimensions = [
                p.service_column,
                p.version_column,
                p.zone_column,
                p.error_column,
            ]
            aliases = ["service", "version", "zone", "errorClass"]
            selection = (
                ", ".join(
                    f"{c(k)} AS {c(a)}"
                    for k, a in zip(dimensions, aliases, strict=True)
                )
                + ', COUNT(*) AS "count"'
            )
            predicates.append(f"{c(p.event_column)} = 'span'")
            suffix = " GROUP BY " + ", ".join(map(c, dimensions))
        elif kind == "watermark":
            selection = (
                f'{c(p.service_column)} AS "service", '
                f'MAX({c(p.time_column)}) AS "watermark_ms"'
            )
            predicates.append(f"{c(p.event_column)} = 'watermark'")
            suffix = f" GROUP BY {c(p.service_column)}"
        elif kind == "changes":
            fields = [
                p.event_id_column,
                p.event_column,
                p.time_column,
                p.service_column,
                p.version_column,
                p.zone_column,
            ]
            selection = ", ".join(map(c, fields))
            predicates.append(
                f"{c(p.event_column)} IN ('deploy', 'deployment', 'traffic_switch', "
                "'experiment', 'config', 'configuration')"
            )
            suffix = f" ORDER BY {c(p.time_column)}, {c(p.event_id_column)}"
        elif kind == "onset":
            if (
                candidate is None
                or candidate.service is None
                or candidate.version is None
            ):
                raise ValueError("Onset requires candidate service and version.")
            selection = f'COUNT(*) AS "count", MIN({c(p.time_column)}) AS "min_eventTs"'
            predicates.extend(
                [
                    f"{c(p.event_column)} = 'span'",
                    f"{c(p.service_column)} = {literal(candidate.service)}",
                    f"{c(p.version_column)} = {literal(candidate.version)}",
                    f"{c(p.error_column)} != 'ok'",
                ]
            )
            if candidate.zone is not None:
                predicates.append(f"{c(p.zone_column)} = {literal(candidate.zone)}")
        elif kind == "trace":
            if p.span_column is None or p.parent_span_column is None:
                raise ValueError(
                    "Trace lookup requires actual span/parent-span columns."
                )
            fields = [
                p.trace_column,
                p.span_column,
                p.parent_span_column,
                p.service_column,
                p.time_column,
                p.error_column,
            ]
            selection = ", ".join(map(c, fields))
            predicates.extend(
                [
                    f"{c(p.event_column)} = 'span'",
                    f"{c(p.trace_column)} = "
                    + literal(_value(trace_id or "", "trace ID")),
                ]
            )
            suffix = f" ORDER BY {c(p.time_column)}, {c(p.span_column)}"
        else:
            raise ValueError("Unsupported incident query kind.")
        table = ".".join(c(part) for part in p.table.split("."))
        # Identifiers are validated/quoted; every variable value is a SQLGlot literal.
        sql = (
            f"SELECT {selection} FROM {table} WHERE {' AND '.join(predicates)}"  # noqa: S608
            f"{suffix} LIMIT {p.max_rows + 1}"
        )
        return sqlglot.parse_one(sql, read="trino").sql(dialect="trino")

    def query(
        self,
        run_id: str,
        kind: QueryKind,
        candidate: IncidentHypothesis | None = None,
        *,
        principal: str,
    ) -> IncidentEvidence:
        if kind != "onset" and candidate is not None:
            raise ValueError("Candidate applies only to onset queries.")
        return self._query(run_id, kind, principal, candidate)

    def get_trace(
        self, run_id: str, trace_id: str, *, principal: str
    ) -> IncidentEvidence:
        return self._query(run_id, "trace", principal, trace_id=trace_id)

    def _query(
        self,
        run_id: str,
        kind: str,
        principal: str,
        candidate: IncidentHypothesis | None = None,
        trace_id: str | None = None,
    ) -> IncidentEvidence:
        with self._lock:
            run = self._get(run_id, principal)
            p = run.profile
            if run.queries >= p.max_queries or run.inflight >= p.max_inflight:
                raise ValueError("Investigation query admission budget exhausted.")
            if (
                run.retained_rows >= p.max_total_rows
                or run.retained_bytes >= p.max_total_response_bytes
            ):
                raise ValueError("Investigation retained-evidence budget exhausted.")
            sql = self._sql(run, kind, candidate, trace_id)
            failure_bytes = self._failure_bytes(sql, kind)
            if (
                failure_bytes > p.max_response_bytes
                or run.retained_bytes + run.reserved_failure_bytes + failure_bytes
                > p.max_total_response_bytes
            ):
                raise ValueError("Investigation retained-evidence budget exhausted.")
            remaining = min(run.deadline, run.created + self._ttl) - self._clock()
            if remaining <= 0:
                raise ValueError("Investigation expired before query submission.")
            run.queries += 1
            run.inflight += 1
            run.reserved_failure_bytes += failure_bytes
        evidence = IncidentEvidence(
            evidence_id="ev_" + secrets.token_hex(16),
            run_id=run_id,
            kind=kind,
            sql=sql,
        )
        try:
            result = self._execute_query(
                sql, max_rows=p.max_rows + 1, timeout_seconds=remaining
            )
            json.dumps(result.model_dump(), allow_nan=False)
            evidence.columns, evidence.rows = result.columns, result.rows
            evidence.metadata = result.metadata.model_copy(deep=True)
            m = evidence.metadata
            expression = sqlglot.parse_one(sql, read="trino")
            if not isinstance(expression, exp.Select):
                raise ValueError("Expected a fixed SELECT query.")
            evidence.complete = (
                m.completeness == "complete"
                and not m.unknown_reasons
                and m.partial_result is not True
                and m.execution_limit_reached is False
                and not any(m.execution_limit_flags.values())
                and not m.early_termination_reasons
                and m.row_limit_reached is False
                and type(m.servers_queried) is int
                and m.servers_queried > 0
                and m.servers_queried == m.servers_responded
                and m.query_sha256 == hashlib.sha256(sql.encode()).hexdigest()
                and len(evidence.rows) <= p.max_rows
                and evidence.columns == expression.named_selects
                and len(set(evidence.columns)) == len(evidence.columns)
                and all(set(row) == set(evidence.columns) for row in evidence.rows)
            )
            if not evidence.complete:
                evidence.error = "Native execution incomplete, unknown, or truncated."
        except Exception as error:
            evidence.columns, evidence.rows = [], []
            evidence.metadata = QueryExecutionMetadata()
            evidence.complete = False
            evidence.error = "Query execution failed: " + type(error).__name__[:64]
        with self._lock:
            try:
                if self._clock() >= min(run.deadline, run.created + self._ttl):
                    evidence.complete, evidence.error = (
                        False,
                        "Query returned after the investigation deadline.",
                    )
                if len(evidence.rows) + run.retained_rows > p.max_total_rows:
                    evidence.complete, evidence.error = False, _BUDGET_ERROR
                if not evidence.complete:
                    evidence.rows = []
                byte_limit = min(
                    p.max_response_bytes,
                    p.max_total_response_bytes
                    - run.retained_bytes
                    - (run.reserved_failure_bytes - failure_bytes),
                )
                stored, delivery, stored_bytes = self._snapshot(evidence, byte_limit)
                # Hashing/serialization/validation are part of the deadline, too.
                if self._clock() >= min(run.deadline, run.created + self._ttl):
                    evidence.complete, evidence.error = (
                        False,
                        "Query returned after the investigation deadline.",
                    )
                    evidence.rows = []
                    stored, delivery, stored_bytes = self._snapshot(
                        evidence, byte_limit
                    )
                run.evidence[evidence.evidence_id] = stored
                run.retained_rows += len(evidence.rows)
                run.retained_bytes += stored_bytes
            finally:
                run.inflight -= 1
                run.reserved_failure_bytes -= failure_bytes
        return delivery

    def finish(
        self,
        run_id: str,
        citations: list[str],
        hypothesis: IncidentHypothesis | None = None,
        status: Literal["proposed", "abstained", "incomplete"] = "proposed",
        reason: str | None = None,
        *,
        principal: str,
    ) -> IncidentFinish:
        if status not in ("proposed", "abstained", "incomplete"):
            raise ValueError("Invalid finish status.")
        if status == "proposed" and hypothesis is None:
            raise ValueError("Proposed requires an explicitly unverified hypothesis.")
        if status != "proposed" and hypothesis is not None:
            raise ValueError("Non-proposed outcomes require a null hypothesis.")
        if reason is not None and (
            not isinstance(reason, str) or not reason.strip() or len(reason) > 4096
        ):
            raise ValueError("Finish reason must contain 1..4096 characters.")
        if status == "incomplete" and reason is None:
            raise ValueError("Incomplete requires a reason.")
        if (
            not isinstance(citations, list)
            or len(citations) > 64
            or any(not isinstance(x, str) for x in citations)
            or len(set(citations)) != len(citations)
        ):
            raise ValueError("Citations must be unique actual evidence IDs.")
        with self._lock:
            run = self._get(run_id, principal)
            if run.inflight:
                raise ValueError("Pending queries prevent finish.")
            if not citations and status != "incomplete":
                raise ValueError("Complete evidence citations are required.")
            selected = {}
            failures = []
            for key, raw in run.evidence.items():
                evidence = IncidentEvidence.model_validate_json(raw)
                digest = hashlib.sha256(
                    json.dumps(
                        evidence.model_dump(exclude={"sha256"}),
                        sort_keys=True,
                        allow_nan=False,
                    ).encode()
                ).hexdigest()
                if digest != evidence.sha256 or evidence.run_id != run_id:
                    raise ValueError("Stored evidence integrity check failed.")
                if not evidence.complete:
                    failures.append(key)
                if key in citations:
                    if not evidence.complete and status != "incomplete":
                        raise ValueError(
                            "Incomplete evidence cannot support this finish."
                        )
                    selected[key] = evidence.sha256
            if set(selected) != set(citations):
                raise ValueError("Forged or foreign evidence citation.")
            finished = IncidentFinish(
                run_id=run_id,
                status=status,
                hypothesis=hypothesis,
                citations=selected,
                reason=reason,
                query_count=run.queries,
                failed_evidence_ids=failures,
            )
            # A potentially lengthy evidence-integrity scan cannot outlive its run.
            self._get(run_id, principal)
            run.closed = True
            return finished
