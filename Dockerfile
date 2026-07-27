# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

FROM ${PYTHON_IMAGE} AS builder

ARG UV_VERSION=0.8.22

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"

# Copy only the files required to build the application. Dependencies are
# resolved from the committed lockfile; --frozen prevents implicit lock updates.
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY mcp_pinot ./mcp_pinot
RUN uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime

LABEL io.modelcontextprotocol.server.name="io.github.startreedata/mcp-pinot"

ENV HOME=/tmp \
    PATH=/app/.venv/bin:${PATH} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XDG_CACHE_HOME=/tmp/.cache \
    XDG_DATA_HOME=/tmp/.local/share

WORKDIR /app

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/config \
    && chown -R 1000:1000 /app

COPY --from=builder /app/.venv /app/.venv
COPY --chmod=0555 run.sh /app/run.sh

USER 1000:1000

ENTRYPOINT ["/app/run.sh"]
