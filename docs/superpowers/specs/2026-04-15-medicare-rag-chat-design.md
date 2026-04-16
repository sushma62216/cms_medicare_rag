# Medicare RAG Chat Application — Design Spec

## Overview

A portfolio demo RAG-powered chat application that lets users ask natural-language analytical questions about 1.1 million CMS Medicare prescriber records. Built on Snowflake Cortex, dbt, FastAPI, and Next.js.

**Audience:** Portfolio/demo project showcasing the full stack.
**Deployment:** Local only.
**Query type:** Analytical/aggregate questions (e.g., "Which specialties prescribe the most antibiotics?").
**Chat UX:** Single-turn Q&A with multi-turn conversation memory.

## Architecture

```
CSV → scripts/load_to_snowflake.py → RAW table
  → dbt staging → dbt marts (5 aggregate tables) → mart_embedded_summaries
  → scripts/generate_embeddings.py → EMBEDDINGS table (VECTOR column)

User question → Next.js → POST /api/chat → FastAPI
  → EMBED_TEXT_768(question) → cosine similarity search → top-K summaries
  → COMPLETE('mistral-large2', prompt + summaries + history)
  → { answer, sources } → Next.js renders in chat
```

## Phase 1 (Current Scope)

### 1. Data Pipeline (dbt + Snowflake)

#### Raw Data Ingestion

- Load the CSV into Snowflake (`RAW_MEDICARE.PRESCRIBER_DATA`) via a `PUT`/`COPY INTO` script at `scripts/load_to_snowflake.py`
- Source columns include: provider NPI, name, credentials, specialty, drug name, generic name, claim count, total cost, beneficiary count, state, etc.

#### dbt Layers

```
models/
  staging/
    stg_prescribers.sql         — clean/rename raw columns, cast types, filter nulls
  marts/
    mart_specialty_drugs.sql     — drug stats aggregated by specialty
    mart_state_summary.sql       — prescribing patterns by state
    mart_top_providers.sql       — top prescribers by volume/cost per specialty
    mart_drug_summary.sql        — per-drug totals (claims, cost, unique prescribers)
    mart_specialty_summary.sql   — per-specialty totals
    mart_embedded_summaries.sql  — converts each mart row into a natural-language text string
```

Each mart produces rows that are converted into natural-language descriptions in `mart_embedded_summaries.sql` using string concatenation. Example output:

> "Specialty: Cardiology. Total prescribers: 4,231. Top drug: Atorvastatin with 128,000 claims costing $14.2M. Second: Lisinopril with 95,000 claims. Most active state: Florida with 612 providers."

#### Embedding Generation

- `scripts/generate_embeddings.py` reads from `mart_embedded_summaries`
- Calls `EMBED_TEXT_768()` on each text description via Snowflake SQL
- Writes results to an `EMBEDDINGS` table with columns: `id`, `summary_type`, `summary_text`, `embedding VECTOR(FLOAT, 768)`
- Expected volume: ~10K-50K summary rows (not 1.1M)

### 2. Backend (FastAPI)

#### Structure

```
backend/app/
  main.py              — FastAPI app, CORS config, lifespan
  config.py            — Pydantic settings (Snowflake creds from .env)
  routers/
    chat.py            — POST /api/chat endpoint
  services/
    snowflake.py       — Snowflake connection, query execution
    retrieval.py       — embed user query, vector similarity search
    generation.py      — build prompt, call COMPLETE(), format response
  models/
    schemas.py         — request/response Pydantic models
```

#### API Contract

**Request:** `POST /api/chat`
```json
{
  "question": "Which specialties prescribe the most opioids?",
  "history": [
    { "role": "user", "content": "..." },
    { "role": "assistant", "content": "..." }
  ]
}
```

**Response:**
```json
{
  "answer": "The top three specialties prescribing opioids are...",
  "sources": [
    {
      "summary_type": "specialty_drugs",
      "summary_text": "Specialty: Pain Management. Top drug: Oxycodone..."
    }
  ]
}
```

#### Request Flow

1. Receive question + conversation history from frontend
2. **Retrieval:** Embed the question with `EMBED_TEXT_768()`, run cosine similarity against `EMBEDDINGS` table, return top-K (5-10) summaries
3. **Generation:** Build prompt with system instructions + retrieved summaries + conversation history + user question. Call `COMPLETE('mistral-large2', prompt)`
4. Return answer + source summaries

#### Key Decisions

- Conversation history managed client-side; frontend sends last 10 turns (5 user + 5 assistant) per request
- Vector search via SQL: `SELECT ... ORDER BY VECTOR_COSINE_SIMILARITY(embedding, query_embedding) DESC LIMIT 10`
- Single Snowflake connection (no pool needed for local demo)
- No streaming in Phase 1; `COMPLETE()` returns full response

### 3. Frontend (Next.js)

#### Structure

```
frontend/
  package.json
  next.config.js
  app/
    layout.tsx           — root layout, fonts, metadata
    page.tsx             — main chat page
  components/
    ChatWindow.tsx       — scrollable message list
    MessageBubble.tsx    — single message (user or assistant)
    ChatInput.tsx        — text input + send button
    SourceCard.tsx       — expandable card showing which summary was used
    SuggestedQuestions.tsx — starter prompts on empty state
  lib/
    api.ts               — fetch wrapper for POST /api/chat
    types.ts             — TypeScript types matching backend schemas
```

#### UX Flow

1. User lands on a clean chat interface titled "Ask Medicare" with 3-4 suggested questions (e.g., "Which specialties prescribe the most opioids?", "What are the top drugs by total cost in California?")
2. User types or clicks a suggestion; message appears in chat with a loading indicator
3. Assistant response appears with the answer text
4. Below each answer, collapsible source cards show which summary chunks informed the response (RAG transparency)
5. Conversation continues with history context maintained in React state

#### Key Decisions

- No streaming — full response rendered on arrival
- State management: React `useState` for messages array
- Styling: Tailwind CSS
- No auth — open access, local demo

### 4. Error Handling

- Backend: FastAPI exception handler returns `{ "error": "message" }` on Snowflake connection or Cortex call failures
- Frontend: displays friendly error message in chat bubble if API call fails
- No retries or circuit breakers

### 5. Testing

- `backend/tests/test_retrieval.py` — unit test: retrieval service builds correct SQL, parses results
- `backend/tests/test_generation.py` — unit test: prompt construction includes history + retrieved chunks
- `backend/tests/test_chat_endpoint.py` — integration test: `/api/chat` endpoint with mocked Snowflake
- dbt: `schema.yml` with `not_null` and `unique` tests on key mart columns
- No frontend tests

## Phase 2 (Future — Not Designed)

- Row-level embeddings (1.1M rows) for provider-specific lookups
- Routing layer to classify question type and pick summary vs. row-level vector search
- Richer visualizations (charts for aggregate results)

## Environment Variables (.env)

```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_ROLE=
```
