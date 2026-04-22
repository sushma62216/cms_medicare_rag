# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

This repository is **scaffolding only** — directories and dependencies exist, but implementation has not started. The authoritative design lives in `docs/superpowers/specs/2026-04-15-medicare-rag-chat-design.md`. **Read the design spec before making non-trivial changes**; it defines layer boundaries, the API contract, key technical decisions (e.g., no streaming in Phase 1, conversation history client-side, single Snowflake connection), and what is explicitly out of scope (Phase 2).

## Architecture

End-to-end RAG pipeline against ~1.1M CMS Medicare prescriber records, all running on Snowflake Cortex:

```
CSV → scripts/load_to_snowflake.py → RAW_MEDICARE.PRESCRIBER_DATA
  → dbt staging (stg_*) → dbt marts (5 aggregate tables) → mart_embedded_summaries
  → scripts/generate_embeddings.py (EMBED_TEXT_768) → EMBEDDINGS table (VECTOR(FLOAT, 768))

User question → Next.js → POST /api/chat → FastAPI
  → retrieval.py: EMBED_TEXT_768(question) + VECTOR_COSINE_SIMILARITY → top-K summaries
  → generation.py: COMPLETE('mistral-large2', system + summaries + history + question)
  → { answer, sources } → Next.js renders message + collapsible source cards
```

Key architectural points that aren't obvious from a single file:

- **Embeddings are over aggregates, not raw rows.** Phase 1 embeds ~10K–50K natural-language summary strings produced by `mart_embedded_summaries.sql`, not the 1.1M raw prescriber rows. Row-level embeddings are explicitly Phase 2.
- **Backend is layered into routers / services / models.** `routers/chat.py` is a thin HTTP layer; retrieval and generation logic lives in `services/`. Snowflake access is centralized in `services/snowflake.py`.
- **Conversation memory is client-side.** Frontend sends the last ~10 turns with each request; backend is stateless.
- **Vector search is pure SQL** via `VECTOR_COSINE_SIMILARITY` — no external vector DB.

## Component Layout

- `backend/app/` — FastAPI app (`main.py`, `config.py`, `routers/`, `services/`, `models/schemas.py`)
- `dbt/models/staging/` and `dbt/models/marts/` — dbt-snowflake transformations; mart names enumerated in the spec
- `scripts/` — one-shot loaders: `load_to_snowflake.py` (CSV → RAW), `generate_embeddings.py` (marts → EMBEDDINGS)
- `cortex/` — reserved for Snowflake Cortex helper SQL/scripts
- `frontend/` — Next.js app (App Router, Tailwind); structure detailed in the spec
- `docs/superpowers/specs/` — design specs; treat as source of truth

## Common Commands

```bash
# Python env (uses .venv at repo root)
source .venv/bin/activate
pip install -r requirements.txt

# Backend dev server
uvicorn backend.app.main:app --reload --port 8000

# Tests
pytest                                      # all tests
pytest backend/tests/test_retrieval.py      # one file
pytest backend/tests/test_chat_endpoint.py::test_name  # one test

# Lint / format
ruff check .
ruff format .

# dbt (run from dbt/ directory; uses dbt-snowflake)
cd dbt && dbt run
cd dbt && dbt run --select stg_prescribers
cd dbt && dbt test

# Frontend (once initialized)
cd frontend && npm install && npm run dev
```

## Environment

Snowflake credentials are read from `.env` at repo root via `pydantic-settings`. Required keys: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`. The `.env` file is gitignored; `.env.example` is the only `.env*` file allowed in git.

## Scope Discipline

This is a portfolio/local demo. The spec deliberately rejects: streaming responses, auth, retry/circuit-breaker logic, frontend tests, connection pooling, and row-level embeddings. Don't add these without checking with the user — they were considered and excluded.
