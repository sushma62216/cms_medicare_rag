# Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For the human executing this plan yourself:** Each task has a short "Why" note that explains *what you're building and why*. Read it, then follow steps in order. Run every verification command and check the output against "Expected". If anything diverges, stop and ask — do not push through.

**Goal:** Load the CMS Medicare Part D Prescriber CSV (~1.1M rows) into Snowflake, transform it into 5 analytical marts plus 1 embedded-summaries mart via dbt, and generate Snowflake Cortex embeddings into a VECTOR column — ending with an `EMBEDDINGS` table the backend can query via cosine similarity.

**Architecture:** CSV → Snowflake stage → `RAW_MEDICARE.PRESCRIBER_DATA` → dbt staging (typed, cleaned) → 5 mart aggregates + `mart_embedded_summaries` (text descriptions) → `EMBED_TEXT_768` → `EMBEDDINGS` table. All transformations are SQL-first; Python is only glue.

**Tech Stack:** Snowflake (warehouse + Cortex), dbt-snowflake 1.8, Python 3.11+ (`snowflake-connector-python`, `pydantic-settings`, `pytest`), shell.

**Reference spec:** `docs/superpowers/specs/2026-04-15-medicare-rag-chat-design.md`

---

## Prerequisites (before Task 0)

- [ ] A Snowflake account you can log into (trial is fine)
- [ ] A role with `CREATE DATABASE`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE STAGE`, and Cortex function access. `ACCOUNTADMIN` is easiest for a personal demo; otherwise `SYSADMIN` with Cortex privileges.
- [ ] The CMS Medicare Part D Prescriber by Provider and Drug CSV downloaded to your laptop. Source: <https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug>. File is ~250 MB uncompressed.
- [ ] `.venv` activated: `source .venv/bin/activate`
- [ ] Dependencies installed: `pip install -r requirements.txt`

---

## File Structure (what you'll create)

```
.env.example                                    # template — committed
cortex/sql/01_raw_schema.sql                    # DDL: RAW_MEDICARE schema + PRESCRIBER_DATA table + file format + stage
scripts/load_to_snowflake.py                    # CSV → stage → RAW table
scripts/generate_embeddings.py                  # mart_embedded_summaries → EMBEDDINGS
scripts/tests/__init__.py                       # empty — marks tests as a package
scripts/tests/test_load_to_snowflake.py         # unit tests for load script
scripts/tests/test_generate_embeddings.py       # unit tests for embeddings script
dbt/dbt_project.yml                             # dbt project config
dbt/profiles.yml                                # dbt connection profile (env-var interpolation)
dbt/models/staging/sources.yml                  # declares RAW_MEDICARE.PRESCRIBER_DATA as a source
dbt/models/staging/schema.yml                   # tests on stg_prescribers
dbt/models/staging/stg_prescribers.sql          # cleaned/typed view over raw
dbt/models/marts/schema.yml                     # tests on all marts
dbt/models/marts/mart_drug_summary.sql          # per-drug totals
dbt/models/marts/mart_specialty_summary.sql     # per-specialty totals
dbt/models/marts/mart_state_summary.sql         # per-state totals
dbt/models/marts/mart_specialty_drugs.sql       # top drugs per specialty
dbt/models/marts/mart_top_providers.sql         # top providers by volume/cost per specialty
dbt/models/marts/mart_embedded_summaries.sql    # UNION ALL → one text column per mart row
```

---

## Task 0: Environment setup — `.env.example`, `.env`, connectivity probe

**Why:** Before writing any code, lock in the secrets layout and *prove* the credentials work. A 30-second probe saves hours of misleading errors later (wrong account URL, missing warehouse, expired trial, etc.).

**Files:**
- Create: `.env.example`
- Modify: `.env` (exists empty — fill with your real values; stays gitignored)

- [ ] **Step 1: Create `.env.example`**

File: `.env.example`
```dotenv
# Snowflake connection
SNOWFLAKE_ACCOUNT=your_account_locator       # e.g. xy12345.us-east-1 or org-account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MEDICARE_RAG
SNOWFLAKE_SCHEMA=ANALYTICS                    # default schema for dbt outputs

# Local file paths
CMS_CSV_PATH=/absolute/path/to/medicare_part_d_prescriber_by_provider_and_drug.csv
```

- [ ] **Step 2: Fill `.env` with your real values.**

Edit `.env` and set each variable. The file is already gitignored. Create the `MEDICARE_RAG` database in Snowflake (via the web UI or SQL below) before continuing.

In the Snowflake web UI, run:
```sql
CREATE DATABASE IF NOT EXISTS MEDICARE_RAG;
CREATE SCHEMA IF NOT EXISTS MEDICARE_RAG.ANALYTICS;
```

- [ ] **Step 3: Write a one-file connectivity probe.**

Create a throwaway file `scripts/probe_snowflake.py` (we'll delete it at the end of this task):

```python
"""Throwaway: confirm Snowflake creds work before writing anything else."""
import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)
with conn.cursor() as cur:
    cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    print(cur.fetchone())
conn.close()
```

- [ ] **Step 4: Run the probe.**

```bash
python scripts/probe_snowflake.py
```

Expected: one tuple printed with your account, role, warehouse, database (`MEDICARE_RAG`), schema (`ANALYTICS`). No exceptions.

If it fails: the error message tells you *exactly* which credential is wrong. Fix `.env` and re-run. Do not continue until this passes.

- [ ] **Step 5: Delete the probe.**

```bash
rm scripts/probe_snowflake.py
```

- [ ] **Step 6: Commit.**

```bash
git add .env.example
git commit -m "chore: add .env.example template for Snowflake credentials"
```

---

## Task 1: Create raw schema, file format, stage, and target table in Snowflake

**Why:** The raw-layer DDL is a separate artifact from application code. Keeping it in `cortex/sql/` (plain SQL you can re-run) makes it trivially reproducible and version-controlled. The `FILE FORMAT` and internal `STAGE` are Snowflake's standard recipe for bulk-loading local CSVs.

**Files:**
- Create: `cortex/sql/01_raw_schema.sql`

- [ ] **Step 1: Write the DDL file.**

File: `cortex/sql/01_raw_schema.sql`
```sql
-- Run this once, in order, against the MEDICARE_RAG database.
-- Columns mirror the CMS Medicare Part D Prescriber by Provider and Drug dataset
-- (CY2022, released 2024). If your CSV differs, adjust column names/types to match.

USE DATABASE MEDICARE_RAG;

CREATE SCHEMA IF NOT EXISTS RAW_MEDICARE;

USE SCHEMA RAW_MEDICARE;

-- File format: CMS CSV is standard-quoted, comma-separated, with a header row.
CREATE OR REPLACE FILE FORMAT CMS_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NA', 'NULL', '*')  -- CMS masks small counts as '*'
  EMPTY_FIELD_AS_NULL = TRUE
  ESCAPE_UNENCLOSED_FIELD = NONE
  TRIM_SPACE = TRUE;

-- Internal stage: a named Snowflake-managed location we PUT the CSV into before COPY.
CREATE OR REPLACE STAGE CMS_RAW_STAGE
  FILE_FORMAT = CMS_CSV_FORMAT;

-- Raw table: one row per (prescriber, drug).
CREATE OR REPLACE TABLE PRESCRIBER_DATA (
  PRSCRBR_NPI             NUMBER(38,0),
  PRSCRBR_LAST_ORG_NAME   VARCHAR(200),
  PRSCRBR_FIRST_NAME      VARCHAR(100),
  PRSCRBR_MI              VARCHAR(10),
  PRSCRBR_CRDNTLS         VARCHAR(100),
  PRSCRBR_GNDR            VARCHAR(10),
  PRSCRBR_ENT_CD          VARCHAR(10),
  PRSCRBR_ST1             VARCHAR(200),
  PRSCRBR_ST2             VARCHAR(200),
  PRSCRBR_CITY            VARCHAR(100),
  PRSCRBR_STATE_ABRVTN    VARCHAR(2),
  PRSCRBR_STATE_FIPS      VARCHAR(10),
  PRSCRBR_ZIP5            VARCHAR(10),
  PRSCRBR_RUCA            VARCHAR(10),
  PRSCRBR_RUCA_DESC       VARCHAR(200),
  PRSCRBR_CNTRY           VARCHAR(10),
  PRSCRBR_TYPE            VARCHAR(200),   -- specialty
  PRSCRBR_TYPE_SRC        VARCHAR(10),
  BRND_NAME               VARCHAR(200),
  GNRC_NAME               VARCHAR(200),
  TOT_CLMS                NUMBER(18,0),
  TOT_30DAY_FILLS         NUMBER(18,2),
  TOT_DAY_SUPLY           NUMBER(18,0),
  TOT_DRUG_CST            NUMBER(18,2),
  TOT_BENES               NUMBER(18,0),
  GE65_SPRSN_FLAG         VARCHAR(10),
  GE65_TOT_CLMS           NUMBER(18,0),
  GE65_TOT_30DAY_FILLS    NUMBER(18,2),
  GE65_TOT_DRUG_CST       NUMBER(18,2),
  GE65_TOT_DAY_SUPLY      NUMBER(18,0),
  GE65_BENE_SPRSN_FLAG    VARCHAR(10),
  GE65_TOT_BENES          NUMBER(18,0)
);
```

- [ ] **Step 2: Preview your CSV's header row to verify column order/names.**

```bash
head -1 "$CMS_CSV_PATH" | tr ',' '\n' | nl
```

Expected: ~32 columns matching (case-insensitive) the order of columns in the DDL above. If your CSV has a different column set or order, **edit the DDL to match exactly** — `COPY INTO` loads positionally.

- [ ] **Step 3: Execute the DDL in Snowflake.**

Paste the contents of `cortex/sql/01_raw_schema.sql` into a Snowflake worksheet and run all statements. Expected: each statement succeeds with a status row (`Table PRESCRIBER_DATA successfully created.`, etc.).

- [ ] **Step 4: Verify the table exists and is empty.**

In Snowflake:
```sql
SELECT COUNT(*) FROM MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA;
```
Expected: `0`.

- [ ] **Step 5: Commit.**

```bash
git add cortex/sql/01_raw_schema.sql
git commit -m "feat(pipeline): add raw schema, stage, file format, and PRESCRIBER_DATA DDL"
```

---

## Task 2: Write `load_to_snowflake.py` with TDD — SQL builders first

**Why:** Bulk-loading is two lines of SQL (`PUT`, then `COPY INTO`), but the string building *is* where bugs live — wrong file URI, missing stage name, typo'd table. Unit-test the builders so you never debug a failed load by re-loading 1.1M rows.

**Files:**
- Create: `scripts/tests/__init__.py`
- Create: `scripts/tests/test_load_to_snowflake.py`
- Create: `scripts/load_to_snowflake.py`

- [ ] **Step 1: Create the empty package marker.**

File: `scripts/tests/__init__.py`
```python
```
(zero bytes — just create the file)

- [ ] **Step 2: Write failing tests for the SQL builders.**

File: `scripts/tests/test_load_to_snowflake.py`
```python
"""Unit tests for the SQL builders in load_to_snowflake.

We only test string construction; actual Snowflake execution is smoke-tested
manually at the end of Task 2.
"""
from scripts.load_to_snowflake import build_put_sql, build_copy_sql


def test_build_put_sql_wraps_local_path_in_file_uri():
    sql = build_put_sql(
        local_path="/tmp/foo.csv",
        stage="@MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE",
    )
    assert sql == (
        "PUT file:///tmp/foo.csv "
        "@MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE "
        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )


def test_build_copy_sql_references_table_stage_and_format():
    sql = build_copy_sql(
        table="MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA",
        stage="@MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE",
        file_pattern="foo.csv.gz",
    )
    assert "COPY INTO MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA" in sql
    assert "FROM @MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE/foo.csv.gz" in sql
    assert "FILE_FORMAT = (FORMAT_NAME = CMS_CSV_FORMAT)" in sql
    assert "ON_ERROR = 'ABORT_STATEMENT'" in sql
```

- [ ] **Step 3: Run the tests — expect ImportError.**

```bash
pytest scripts/tests/test_load_to_snowflake.py -v
```

Expected: failure with `ModuleNotFoundError: No module named 'scripts.load_to_snowflake'` (or `ImportError` on `build_put_sql`). Good — red phase confirmed.

- [ ] **Step 4: Implement the script (builders + main).**

File: `scripts/load_to_snowflake.py`
```python
"""Load the CMS Medicare Part D Prescriber CSV into Snowflake.

Usage:
    python scripts/load_to_snowflake.py

Reads config from .env. Performs:
  1. PUT the local CSV into the internal stage (gzip-compresses on upload).
  2. COPY INTO PRESCRIBER_DATA.
  3. Reports row count.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

STAGE = "@MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE"
TABLE = "MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA"


def build_put_sql(local_path: str, stage: str) -> str:
    """PUT uploads a local file into a Snowflake stage. AUTO_COMPRESS gzip's it."""
    return f"PUT file://{local_path} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"


def build_copy_sql(table: str, stage: str, file_pattern: str) -> str:
    """COPY INTO moves data from stage to table using the file format on the stage."""
    return (
        f"COPY INTO {table}\n"
        f"FROM {stage}/{file_pattern}\n"
        f"FILE_FORMAT = (FORMAT_NAME = CMS_CSV_FORMAT)\n"
        f"ON_ERROR = 'ABORT_STATEMENT'"
    )


def main() -> None:
    csv_path = os.environ.get("CMS_CSV_PATH")
    if not csv_path or not Path(csv_path).is_file():
        sys.exit(f"CMS_CSV_PATH is not set or does not point to a file: {csv_path!r}")

    file_name_gz = Path(csv_path).name + ".gz"  # PUT + AUTO_COMPRESS appends .gz

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    try:
        with conn.cursor() as cur:
            print(f"[1/3] PUT {csv_path} -> {STAGE}")
            cur.execute(build_put_sql(csv_path, STAGE))
            for row in cur.fetchall():
                print("      ", row)

            print(f"[2/3] COPY INTO {TABLE}")
            cur.execute(build_copy_sql(TABLE, STAGE, file_name_gz))
            for row in cur.fetchall():
                print("      ", row)

            print("[3/3] Row count")
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            (count,) = cur.fetchone()
            print(f"       loaded rows = {count:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the tests — expect pass.**

```bash
pytest scripts/tests/test_load_to_snowflake.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Run the loader against Snowflake.**

```bash
python scripts/load_to_snowflake.py
```

Expected output (numbers will differ):
```
[1/3] PUT /abs/path/medicare....csv -> @MEDICARE_RAG.RAW_MEDICARE.CMS_RAW_STAGE
       ('medicare....csv', 'medicare....csv.gz', 256_123_456, 45_678_901, 'NONE', 'GZIP', 'UPLOADED', '')
[2/3] COPY INTO MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA
       ('medicare....csv.gz', 'LOADED', 1_100_000, 1_100_000, ...)
[3/3] Row count
       loaded rows = 1,100,000
```

The `PUT` step uploads the file (can take a few minutes depending on your uplink). `COPY INTO` then loads it and reports how many rows landed.

If any step fails, **stop and ask** — common issues: wrong column count in DDL vs CSV, role lacking `INSERT` on the table, warehouse not running.

- [ ] **Step 7: Sanity-check the data in Snowflake.**

In a Snowflake worksheet:
```sql
SELECT COUNT(*), COUNT(DISTINCT PRSCRBR_NPI), COUNT(DISTINCT PRSCRBR_TYPE)
FROM MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA;

SELECT * FROM MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA LIMIT 5;
```

Expected: ~1.1M rows, ~500K+ distinct NPIs, ~200+ specialties. First five rows look plausible (names, drugs, numeric claim counts).

- [ ] **Step 8: Commit.**

```bash
git add scripts/load_to_snowflake.py scripts/tests/__init__.py scripts/tests/test_load_to_snowflake.py
git commit -m "feat(pipeline): add CSV loader with SQL builder unit tests"
```

---

## Task 3: dbt project scaffolding — `dbt_project.yml`, `profiles.yml`, `sources.yml`

**Why:** dbt needs three config files before it can run a single model. Defining the `source` explicitly (rather than hardcoding `RAW_MEDICARE.PRESCRIBER_DATA` in every model) lets dbt track lineage and gives you one place to change if the raw table moves.

**Files:**
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`
- Create: `dbt/models/staging/sources.yml`

- [ ] **Step 1: Write `dbt_project.yml`.**

File: `dbt/dbt_project.yml`
```yaml
name: medicare_rag
version: 1.0.0
config-version: 2

profile: medicare_rag

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  medicare_rag:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

- [ ] **Step 2: Write `profiles.yml` (uses env-var interpolation so no secrets are checked in).**

File: `dbt/profiles.yml`
```yaml
medicare_rag:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      threads: 4
      client_session_keep_alive: False
```

- [ ] **Step 3: Write the source declaration.**

File: `dbt/models/staging/sources.yml`
```yaml
version: 2

sources:
  - name: raw_medicare
    database: MEDICARE_RAG
    schema: RAW_MEDICARE
    tables:
      - name: prescriber_data
        description: "Raw CMS Medicare Part D Prescriber by Provider and Drug (CY2022)."
        columns:
          - name: PRSCRBR_NPI
            description: National Provider Identifier.
            tests:
              - not_null
          - name: BRND_NAME
            description: Brand name of the drug.
          - name: GNRC_NAME
            description: Generic name of the drug.
          - name: TOT_CLMS
            description: Total claim count.
```

- [ ] **Step 4: Confirm dbt can connect.**

`dbt` reads env vars from your shell, not `.env`. Export them for this session:

```bash
set -a && source .env && set +a
cd dbt && dbt debug --profiles-dir .
```

Expected: `All checks passed!` — dbt will test config, dependencies, Git, and the Snowflake connection.

If it fails: `dbt debug` prints exactly which check failed. Fix and re-run. Common issue: `profiles-dir` not picked up — always pass `--profiles-dir .` when inside `dbt/`.

- [ ] **Step 5: Confirm dbt sees the source.**

Still inside `dbt/`:
```bash
dbt ls --profiles-dir . --resource-type source
```

Expected:
```
source:medicare_rag.raw_medicare.prescriber_data
```

- [ ] **Step 6: Commit.**

```bash
cd ..
git add dbt/dbt_project.yml dbt/profiles.yml dbt/models/staging/sources.yml
git commit -m "feat(dbt): scaffold project config, profile, and raw source"
```

---

## Task 4: Staging model `stg_prescribers` + schema tests

**Why:** The staging layer is a type-clean, renamed, null-filtered view over raw. All downstream marts read from `stg_prescribers`, never directly from raw. This isolates "bad data handling" in one place and makes the mart SQL much easier to read.

**Files:**
- Create: `dbt/models/staging/stg_prescribers.sql`
- Create: `dbt/models/staging/schema.yml`

- [ ] **Step 1: Write the staging model.**

File: `dbt/models/staging/stg_prescribers.sql`
```sql
-- Clean, typed, friendlier-named view over RAW_MEDICARE.PRESCRIBER_DATA.
-- Drops rows with a null NPI, specialty, or drug — those are unusable downstream.
WITH source AS (
    SELECT * FROM {{ source('raw_medicare', 'prescriber_data') }}
),

renamed AS (
    SELECT
        PRSCRBR_NPI                                  AS npi,
        TRIM(PRSCRBR_LAST_ORG_NAME)                  AS last_or_org_name,
        TRIM(PRSCRBR_FIRST_NAME)                     AS first_name,
        TRIM(PRSCRBR_CRDNTLS)                        AS credentials,
        TRIM(PRSCRBR_TYPE)                           AS specialty,
        TRIM(PRSCRBR_STATE_ABRVTN)                   AS state,
        TRIM(PRSCRBR_CITY)                           AS city,
        TRIM(BRND_NAME)                              AS brand_name,
        TRIM(GNRC_NAME)                              AS generic_name,
        COALESCE(TOT_CLMS, 0)                        AS total_claims,
        COALESCE(TOT_DRUG_CST, 0)                    AS total_cost,
        COALESCE(TOT_BENES, 0)                       AS total_beneficiaries,
        COALESCE(TOT_DAY_SUPLY, 0)                   AS total_day_supply
    FROM source
    WHERE PRSCRBR_NPI IS NOT NULL
      AND PRSCRBR_TYPE IS NOT NULL
      AND GNRC_NAME IS NOT NULL
)

SELECT * FROM renamed
```

- [ ] **Step 2: Write schema tests.**

File: `dbt/models/staging/schema.yml`
```yaml
version: 2

models:
  - name: stg_prescribers
    description: "Cleaned, typed staging of CMS Medicare Part D prescriber-by-drug rows."
    columns:
      - name: npi
        description: National Provider Identifier.
        tests:
          - not_null
      - name: specialty
        description: Prescriber specialty (e.g., Internal Medicine).
        tests:
          - not_null
      - name: generic_name
        description: Generic drug name.
        tests:
          - not_null
      - name: total_claims
        description: Total claim count (>= 0).
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select stg_prescribers --profiles-dir .
dbt test --select stg_prescribers --profiles-dir .
```

Expected: `Completed successfully`. The `run` creates a view at `MEDICARE_RAG.ANALYTICS.stg_prescribers`. All 4 tests pass.

- [ ] **Step 4: Sanity-check row count.**

In Snowflake:
```sql
SELECT COUNT(*) FROM MEDICARE_RAG.ANALYTICS.STG_PRESCRIBERS;
SELECT * FROM MEDICARE_RAG.ANALYTICS.STG_PRESCRIBERS LIMIT 5;
```

Expected: a bit less than the raw row count (null-filtered), but close.

- [ ] **Step 5: Commit.**

```bash
cd ..
git add dbt/models/staging/stg_prescribers.sql dbt/models/staging/schema.yml
git commit -m "feat(dbt): add stg_prescribers with null-filter, trim, coalesce"
```

---

## Task 5: Mart `mart_drug_summary` — per-drug totals

**Why:** First mart. Aggregates per generic drug: total claims, total cost, unique prescribers. One row per generic drug (~3K–5K rows) — the right granularity for "which drugs are prescribed most" questions.

**Files:**
- Create: `dbt/models/marts/mart_drug_summary.sql`
- Create: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_drug_summary.sql`
```sql
-- One row per generic drug.
-- Use generic_name (not brand) to collapse brand variants of the same molecule.
SELECT
    generic_name,
    SUM(total_claims)                   AS total_claims,
    SUM(total_cost)                     AS total_cost,
    COUNT(DISTINCT npi)                 AS unique_prescribers,
    SUM(total_beneficiaries)            AS total_beneficiaries
FROM {{ ref('stg_prescribers') }}
GROUP BY generic_name
```

- [ ] **Step 2: Create the marts schema file with tests for this mart.**

File: `dbt/models/marts/schema.yml`
```yaml
version: 2

models:
  - name: mart_drug_summary
    description: "One row per generic drug with aggregate claims, cost, and prescriber counts."
    columns:
      - name: generic_name
        tests:
          - not_null
          - unique
      - name: total_claims
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_drug_summary --profiles-dir .
dbt test --select mart_drug_summary --profiles-dir .
```

Expected: `Completed successfully`. Table `MEDICARE_RAG.ANALYTICS.mart_drug_summary` exists, both tests pass.

- [ ] **Step 4: Spot-check.**

```sql
SELECT * FROM MEDICARE_RAG.ANALYTICS.MART_DRUG_SUMMARY
ORDER BY total_claims DESC LIMIT 10;
```

Expected: common drugs (atorvastatin, lisinopril, levothyroxine, etc.) at the top with millions of claims.

- [ ] **Step 5: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_drug_summary.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_drug_summary aggregating claims/cost per generic drug"
```

---

## Task 6: Mart `mart_specialty_summary` — per-specialty totals

**Why:** One row per specialty (~200 rows). Answers "which specialties prescribe the most, at the highest cost" questions.

**Files:**
- Create: `dbt/models/marts/mart_specialty_summary.sql`
- Modify: `dbt/models/marts/schema.yml` — add tests for this mart

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_specialty_summary.sql`
```sql
-- One row per specialty with aggregate prescribing volume and prescriber count.
SELECT
    specialty,
    SUM(total_claims)                   AS total_claims,
    SUM(total_cost)                     AS total_cost,
    COUNT(DISTINCT npi)                 AS unique_prescribers,
    COUNT(DISTINCT generic_name)        AS unique_drugs
FROM {{ ref('stg_prescribers') }}
GROUP BY specialty
```

- [ ] **Step 2: Append tests to `schema.yml`.**

Append under `models:` in `dbt/models/marts/schema.yml`:
```yaml
  - name: mart_specialty_summary
    description: "One row per specialty with aggregate claims, cost, prescriber and drug counts."
    columns:
      - name: specialty
        tests:
          - not_null
          - unique
      - name: total_claims
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_specialty_summary --profiles-dir .
dbt test --select mart_specialty_summary --profiles-dir .
```

Expected: `Completed successfully`.

- [ ] **Step 4: Spot-check.**

```sql
SELECT * FROM MEDICARE_RAG.ANALYTICS.MART_SPECIALTY_SUMMARY
ORDER BY total_claims DESC LIMIT 10;
```

Expected: Internal Medicine, Family Practice, Nurse Practitioner at the top.

- [ ] **Step 5: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_specialty_summary.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_specialty_summary"
```

---

## Task 7: Mart `mart_state_summary` — per-state totals

**Why:** One row per US state (~50 rows). Enables "top prescribers / most costly drugs in California" style questions via the combined-filter marts that reference it.

**Files:**
- Create: `dbt/models/marts/mart_state_summary.sql`
- Modify: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_state_summary.sql`
```sql
-- One row per state with aggregate prescribing volume and prescriber count.
SELECT
    state,
    SUM(total_claims)                   AS total_claims,
    SUM(total_cost)                     AS total_cost,
    COUNT(DISTINCT npi)                 AS unique_prescribers,
    COUNT(DISTINCT generic_name)        AS unique_drugs
FROM {{ ref('stg_prescribers') }}
WHERE state IS NOT NULL
GROUP BY state
```

- [ ] **Step 2: Append tests to `schema.yml`.**

Append:
```yaml
  - name: mart_state_summary
    description: "One row per state with aggregate prescribing metrics."
    columns:
      - name: state
        tests:
          - not_null
          - unique
      - name: total_claims
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_state_summary --profiles-dir .
dbt test --select mart_state_summary --profiles-dir .
```

Expected: pass. Row count should be ~50–60 (includes DC, territories).

- [ ] **Step 4: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_state_summary.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_state_summary"
```

---

## Task 8: Mart `mart_specialty_drugs` — top drugs per specialty

**Why:** Cross-grain mart answering "what does [specialty] prescribe most?" — one row per (specialty, generic_drug) restricted to top 10 per specialty. ~2K rows total.

**Files:**
- Create: `dbt/models/marts/mart_specialty_drugs.sql`
- Modify: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_specialty_drugs.sql`
```sql
-- Top 10 drugs (by claim count) per specialty.
-- Window function partitions by specialty; outer filter keeps the top 10.
WITH specialty_drug_totals AS (
    SELECT
        specialty,
        generic_name,
        SUM(total_claims)               AS total_claims,
        SUM(total_cost)                 AS total_cost,
        COUNT(DISTINCT npi)             AS unique_prescribers
    FROM {{ ref('stg_prescribers') }}
    GROUP BY specialty, generic_name
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY specialty
            ORDER BY total_claims DESC
        ) AS rank_in_specialty
    FROM specialty_drug_totals
)

SELECT
    specialty,
    generic_name,
    total_claims,
    total_cost,
    unique_prescribers,
    rank_in_specialty
FROM ranked
WHERE rank_in_specialty <= 10
```

- [ ] **Step 2: Append tests to `schema.yml`.**

Append under `models:` in `dbt/models/marts/schema.yml`:
```yaml
  - name: mart_specialty_drugs
    description: "Top 10 drugs per specialty by claim count."
    columns:
      - name: specialty
        tests:
          - not_null
      - name: generic_name
        tests:
          - not_null
      - name: rank_in_specialty
        tests:
          - not_null
```

(We deliberately avoid the `dbt_utils` package here to keep dependencies lean for a learning project. A uniqueness test on the `(specialty, generic_name)` pair would normally live in `dbt_utils.unique_combination_of_columns` — worth adding later if you install the package.)

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_specialty_drugs --profiles-dir .
dbt test --select mart_specialty_drugs --profiles-dir .
```

Expected: pass.

- [ ] **Step 4: Spot-check.**

```sql
SELECT * FROM MEDICARE_RAG.ANALYTICS.MART_SPECIALTY_DRUGS
WHERE specialty = 'Cardiology'
ORDER BY rank_in_specialty;
```

Expected: 10 rows with cardiology-typical drugs (atorvastatin, metoprolol, lisinopril, etc.) in descending claim order, ranks 1–10.

- [ ] **Step 5: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_specialty_drugs.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_specialty_drugs (top 10 drugs per specialty)"
```

---

## Task 9: Mart `mart_top_providers` — top providers per specialty

**Why:** Names the leading prescribers per specialty. ~2K rows (top 10 per ~200 specialties). Retrieved when a question mentions a specific specialty and asks "who prescribes the most / costs the most".

**Files:**
- Create: `dbt/models/marts/mart_top_providers.sql`
- Modify: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_top_providers.sql`
```sql
-- Top 10 providers per specialty by total claims.
-- Provider granularity = one row per NPI (summed across all their drugs).
WITH provider_totals AS (
    SELECT
        npi,
        specialty,
        state,
        MAX(last_or_org_name)           AS last_or_org_name,
        MAX(first_name)                 AS first_name,
        MAX(credentials)                AS credentials,
        SUM(total_claims)               AS total_claims,
        SUM(total_cost)                 AS total_cost,
        COUNT(DISTINCT generic_name)    AS unique_drugs
    FROM {{ ref('stg_prescribers') }}
    GROUP BY npi, specialty, state
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY specialty
            ORDER BY total_claims DESC
        ) AS rank_in_specialty
    FROM provider_totals
)

SELECT
    npi,
    specialty,
    state,
    last_or_org_name,
    first_name,
    credentials,
    total_claims,
    total_cost,
    unique_drugs,
    rank_in_specialty
FROM ranked
WHERE rank_in_specialty <= 10
```

- [ ] **Step 2: Append tests to `schema.yml`.**

```yaml
  - name: mart_top_providers
    description: "Top 10 providers per specialty by total claims."
    columns:
      - name: npi
        tests:
          - not_null
      - name: specialty
        tests:
          - not_null
      - name: rank_in_specialty
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_top_providers --profiles-dir .
dbt test --select mart_top_providers --profiles-dir .
```

Expected: pass.

- [ ] **Step 4: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_top_providers.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_top_providers"
```

---

## Task 10: Mart `mart_embedded_summaries` — UNION ALL of text descriptions

**Why:** This is the *only* mart the embedding step reads from. Each row has `summary_type` (which mart it came from), a business-key `entity_key`, and `summary_text` — a plain-English sentence constructed by string concatenation. The LLM gets these strings back at retrieval time, so they need to read like short paragraphs, not key-value dumps.

**Files:**
- Create: `dbt/models/marts/mart_embedded_summaries.sql`
- Modify: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Write the mart.**

File: `dbt/models/marts/mart_embedded_summaries.sql`
```sql
-- One row per embeddable summary. Each row becomes a single vector.
-- summary_type = the source mart; summary_text = natural-language description.
-- entity_key = a human-readable handle so the backend can show it in source cards.

WITH drug AS (
    SELECT
        'drug_summary'                                                      AS summary_type,
        generic_name                                                        AS entity_key,
        'Drug: ' || generic_name
            || '. Total claims: ' || TO_VARCHAR(total_claims, '999,999,999')
            || '. Total cost: $' || TO_VARCHAR(total_cost, '999,999,999,999.00')
            || '. Unique prescribers: ' || TO_VARCHAR(unique_prescribers, '999,999')
            || '. Beneficiaries: ' || TO_VARCHAR(total_beneficiaries, '999,999,999') || '.'
                                                                            AS summary_text
    FROM {{ ref('mart_drug_summary') }}
),

specialty AS (
    SELECT
        'specialty_summary'                                                 AS summary_type,
        specialty                                                           AS entity_key,
        'Specialty: ' || specialty
            || '. Total prescribers: ' || TO_VARCHAR(unique_prescribers, '999,999')
            || '. Total claims: ' || TO_VARCHAR(total_claims, '999,999,999')
            || '. Total cost: $' || TO_VARCHAR(total_cost, '999,999,999,999.00')
            || '. Distinct drugs prescribed: ' || TO_VARCHAR(unique_drugs, '999,999') || '.'
                                                                            AS summary_text
    FROM {{ ref('mart_specialty_summary') }}
),

state AS (
    SELECT
        'state_summary'                                                     AS summary_type,
        state                                                               AS entity_key,
        'State: ' || state
            || '. Active Medicare Part D prescribers: ' || TO_VARCHAR(unique_prescribers, '999,999')
            || '. Total claims: ' || TO_VARCHAR(total_claims, '999,999,999')
            || '. Total cost: $' || TO_VARCHAR(total_cost, '999,999,999,999.00')
            || '. Distinct drugs: ' || TO_VARCHAR(unique_drugs, '999,999') || '.'
                                                                            AS summary_text
    FROM {{ ref('mart_state_summary') }}
),

specialty_drug AS (
    SELECT
        'specialty_drug'                                                    AS summary_type,
        specialty || ' | ' || generic_name                                  AS entity_key,
        'In ' || specialty
            || ', the #' || TO_VARCHAR(rank_in_specialty)
            || ' most-prescribed drug is ' || generic_name
            || ' with ' || TO_VARCHAR(total_claims, '999,999,999') || ' claims'
            || ', $' || TO_VARCHAR(total_cost, '999,999,999,999.00') || ' total cost'
            || ', prescribed by ' || TO_VARCHAR(unique_prescribers, '999,999') || ' providers.'
                                                                            AS summary_text
    FROM {{ ref('mart_specialty_drugs') }}
),

top_provider AS (
    SELECT
        'top_provider'                                                      AS summary_type,
        TO_VARCHAR(npi) || ' | ' || specialty                               AS entity_key,
        'Provider ' || COALESCE(first_name || ' ', '') || last_or_org_name
            || COALESCE(', ' || credentials, '')
            || ' (NPI ' || TO_VARCHAR(npi) || ', ' || specialty
            || COALESCE(', ' || state, '') || ')'
            || ' ranks #' || TO_VARCHAR(rank_in_specialty) || ' in their specialty'
            || ' with ' || TO_VARCHAR(total_claims, '999,999,999') || ' claims'
            || ' across ' || TO_VARCHAR(unique_drugs, '999,999') || ' distinct drugs'
            || ' (total cost $' || TO_VARCHAR(total_cost, '999,999,999,999.00') || ').'
                                                                            AS summary_text
    FROM {{ ref('mart_top_providers') }}
),

unioned AS (
    SELECT * FROM drug
    UNION ALL SELECT * FROM specialty
    UNION ALL SELECT * FROM state
    UNION ALL SELECT * FROM specialty_drug
    UNION ALL SELECT * FROM top_provider
)

SELECT
    ROW_NUMBER() OVER (ORDER BY summary_type, entity_key) AS id,
    summary_type,
    entity_key,
    summary_text
FROM unioned
```

- [ ] **Step 2: Append tests to `schema.yml`.**

```yaml
  - name: mart_embedded_summaries
    description: "One row per embeddable natural-language summary (union of all marts)."
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: summary_type
        tests:
          - not_null
          - accepted_values:
              values: ['drug_summary', 'specialty_summary', 'state_summary', 'specialty_drug', 'top_provider']
      - name: summary_text
        tests:
          - not_null
```

- [ ] **Step 3: Run and test.**

```bash
cd dbt && dbt run --select mart_embedded_summaries --profiles-dir .
dbt test --select mart_embedded_summaries --profiles-dir .
```

Expected: pass. Table has roughly `|drugs| + |specialties| + |states| + |specialty×drug top10| + |specialty×provider top10|` rows — order of 10K–30K.

- [ ] **Step 4: Read a few summaries to confirm they parse as English.**

```sql
SELECT summary_type, summary_text
FROM MEDICARE_RAG.ANALYTICS.MART_EMBEDDED_SUMMARIES
WHERE summary_type = 'specialty_drug'
LIMIT 5;
```

Expected: sentences like *"In Cardiology, the #1 most-prescribed drug is atorvastatin calcium with 128,000 claims, $14,200,000.00 total cost, prescribed by 4,231 providers."*

If the formatting looks ugly (e.g., stray commas, weird number formatting), tweak the `TO_VARCHAR` masks and re-run. Summary quality *directly* determines retrieval quality.

- [ ] **Step 5: Commit.**

```bash
cd ..
git add dbt/models/marts/mart_embedded_summaries.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_embedded_summaries with natural-language descriptions"
```

---

## Task 11: `generate_embeddings.py` — call `EMBED_TEXT_768` over the summaries

**Why:** One SQL statement does all the work: `CREATE TABLE EMBEDDINGS AS SELECT id, ..., SNOWFLAKE.CORTEX.EMBED_TEXT_768(model, summary_text) FROM mart_embedded_summaries`. The Python script is just orchestration + a verify query. We TDD the SQL builder so the embedding model name, source table, and vector column name are locked in by tests.

**Files:**
- Create: `scripts/tests/test_generate_embeddings.py`
- Create: `scripts/generate_embeddings.py`

- [ ] **Step 1: Write failing tests.**

File: `scripts/tests/test_generate_embeddings.py`
```python
"""Unit tests for the embeddings SQL builder."""
from scripts.generate_embeddings import build_embeddings_ddl


def test_build_embeddings_ddl_references_model_source_and_target():
    sql = build_embeddings_ddl(
        target_table="MEDICARE_RAG.ANALYTICS.EMBEDDINGS",
        source_table="MEDICARE_RAG.ANALYTICS.MART_EMBEDDED_SUMMARIES",
        model="snowflake-arctic-embed-m-v1.5",
    )
    assert "CREATE OR REPLACE TABLE MEDICARE_RAG.ANALYTICS.EMBEDDINGS" in sql
    assert "FROM MEDICARE_RAG.ANALYTICS.MART_EMBEDDED_SUMMARIES" in sql
    assert (
        "SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', summary_text)"
        in sql
    )
    assert "embedding" in sql


def test_build_embeddings_ddl_preserves_id_and_metadata_columns():
    sql = build_embeddings_ddl(
        target_table="T",
        source_table="S",
        model="m",
    )
    # Required pass-through columns so the backend can show the source text back:
    for col in ("id", "summary_type", "entity_key", "summary_text"):
        assert col in sql
```

- [ ] **Step 2: Run tests — expect ImportError.**

```bash
pytest scripts/tests/test_generate_embeddings.py -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Write the script.**

File: `scripts/generate_embeddings.py`
```python
"""Populate the EMBEDDINGS table from mart_embedded_summaries.

Runs a single `CREATE OR REPLACE TABLE ... AS SELECT ..., EMBED_TEXT_768(...)` —
Snowflake Cortex embeds every row in one pass (it's batch-parallelized server-side).

Usage:
    python scripts/generate_embeddings.py
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

EMBEDDING_MODEL = "snowflake-arctic-embed-m-v1.5"  # 768-dim, Snowflake-native


def build_embeddings_ddl(target_table: str, source_table: str, model: str) -> str:
    return (
        f"CREATE OR REPLACE TABLE {target_table} AS\n"
        f"SELECT\n"
        f"    id,\n"
        f"    summary_type,\n"
        f"    entity_key,\n"
        f"    summary_text,\n"
        f"    SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', summary_text) AS embedding\n"
        f"FROM {source_table}"
    )


def main() -> None:
    db = os.environ["SNOWFLAKE_DATABASE"]
    schema = os.environ["SNOWFLAKE_SCHEMA"]
    target = f"{db}.{schema}.EMBEDDINGS"
    source = f"{db}.{schema}.MART_EMBEDDED_SUMMARIES"

    sql = build_embeddings_ddl(target, source, EMBEDDING_MODEL)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=db,
        schema=schema,
    )
    try:
        with conn.cursor() as cur:
            print(f"[1/2] Building embeddings into {target} using model {EMBEDDING_MODEL}")
            print("      (this runs one SQL statement — Cortex handles batching)")
            cur.execute(sql)
            print("      ", cur.fetchone())

            print("[2/2] Row count + sample")
            cur.execute(f"SELECT COUNT(*) FROM {target}")
            (count,) = cur.fetchone()
            print(f"       embedded rows = {count:,}")

            cur.execute(
                f"SELECT summary_type, LEFT(summary_text, 80), "
                f"ARRAY_SIZE(embedding::ARRAY) AS dim "
                f"FROM {target} LIMIT 3"
            )
            for row in cur.fetchall():
                print("      ", row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests — expect pass.**

```bash
pytest scripts/tests/test_generate_embeddings.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Run the script against Snowflake.**

```bash
python scripts/generate_embeddings.py
```

Expected output:
```
[1/2] Building embeddings into MEDICARE_RAG.ANALYTICS.EMBEDDINGS using model snowflake-arctic-embed-m-v1.5
       (this runs one SQL statement — Cortex handles batching)
       ('Table EMBEDDINGS successfully created.',)
[2/2] Row count + sample
       embedded rows = ~15,000
       ('drug_summary', 'Drug: ATORVASTATIN CALCIUM. Total claims: ...', 768)
       ...
```

**Key check:** `dim = 768` on each row. If you see a different number, the model name is wrong for `EMBED_TEXT_768`.

This step is the **most expensive** of the pipeline — budget 1–5 minutes of warehouse time depending on row count and warehouse size.

- [ ] **Step 6: Verify the table in Snowflake.**

```sql
SELECT COUNT(*), COUNT(DISTINCT summary_type)
FROM MEDICARE_RAG.ANALYTICS.EMBEDDINGS;

SELECT summary_type, summary_text,
       VECTOR_COSINE_SIMILARITY(
           embedding,
           SNOWFLAKE.CORTEX.EMBED_TEXT_768(
               'snowflake-arctic-embed-m-v1.5',
               'Which specialties prescribe the most opioids?'
           )
       ) AS score
FROM MEDICARE_RAG.ANALYTICS.EMBEDDINGS
ORDER BY score DESC
LIMIT 5;
```

Expected: the top 5 are pain-management / anesthesiology / psychiatry summaries, or opioid-drug summaries (oxycodone, hydrocodone, etc.). This is your first end-to-end proof the retrieval layer will work.

- [ ] **Step 7: Commit.**

```bash
git add scripts/generate_embeddings.py scripts/tests/test_generate_embeddings.py
git commit -m "feat(pipeline): add Cortex embedding generation over mart_embedded_summaries"
```

---

## Task 12: End-to-end full-pipeline rerun from scratch

**Why:** You've been running steps piecemeal. A single "cold start" rerun proves the pipeline is idempotent and will still work after a reboot or a month from now. This is the "acceptance test" for Plan 1.

**Files:** none — this task only runs existing artifacts.

- [ ] **Step 1: Run the whole dbt graph.**

```bash
set -a && source .env && set +a
cd dbt && dbt build --profiles-dir .
```

`dbt build` = `run` + `test`, in dependency order. Expected: `Completed successfully` with 0 errors, 0 warnings. All tests pass.

- [ ] **Step 2: Regenerate embeddings.**

```bash
cd .. && python scripts/generate_embeddings.py
```

Expected: same row count as before, `dim = 768`.

- [ ] **Step 3: Run all Python unit tests.**

```bash
pytest scripts/tests/ -v
```

Expected: 4 passed.

- [ ] **Step 4: Final end-to-end smoke query.**

```sql
-- Plug in any question you'd like to ask; should return plausible top-5 summaries.
SELECT summary_type, LEFT(summary_text, 120) AS preview,
       ROUND(VECTOR_COSINE_SIMILARITY(
           embedding,
           SNOWFLAKE.CORTEX.EMBED_TEXT_768(
               'snowflake-arctic-embed-m-v1.5',
               'What are the top drugs by total cost in California?'
           )
       ), 4) AS score
FROM MEDICARE_RAG.ANALYTICS.EMBEDDINGS
ORDER BY score DESC
LIMIT 5;
```

Expected: California state_summary + high-cost drug_summary rows near the top, scores in the 0.6–0.9 range.

- [ ] **Step 5: Tag the pipeline as complete.**

```bash
git tag pipeline-v1
git log --oneline -20   # visual review
```

---

## Done — what you've built

- Raw Snowflake schema + loaded 1.1M rows at `MEDICARE_RAG.RAW_MEDICARE.PRESCRIBER_DATA`
- 1 dbt staging view + 5 analytical marts + 1 embedded-summaries mart
- Cortex-generated 768-dim embeddings in `MEDICARE_RAG.ANALYTICS.EMBEDDINGS`
- 4 unit tests, 20+ dbt tests
- Fully idempotent: `dbt build` + `generate_embeddings.py` recreate everything

## What's next

Plan 2 (chat backend) will read `EMBEDDINGS` via `VECTOR_COSINE_SIMILARITY` for retrieval and call `COMPLETE('mistral-large2', ...)` for generation. Plan 3 is the Next.js frontend.

## If things go wrong

- **Wrong column count on COPY INTO** → the CSV header doesn't match `PRESCRIBER_DATA` DDL. Re-run Task 1 Step 2 and adjust DDL.
- **`dbt debug` fails on connection** → env vars not exported in the current shell. Re-run `set -a && source .env && set +a`.
- **`EMBED_TEXT_768` errors "model not available"** → your role lacks Cortex privileges, or your region doesn't host that model. Try `e5-base-v2` as a fallback (also 768-dim).
- **Weirdly formatted summary text** → tweak the `TO_VARCHAR` masks in `mart_embedded_summaries.sql` and re-run `dbt run --select mart_embedded_summaries` + `python scripts/generate_embeddings.py` to refresh vectors.
