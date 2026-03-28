# NFA Forgotten Archive – Agent Knowledge Base

## Project Overview

A personal chat archive analysis tool. Ingests exported group chat logs (WeChat, etc.),
stores them in PostgreSQL, and runs rule-based topic classification to build a
searchable, analysable record of past conversations.

## Tech Stack

- **Language**: Python 3.12
- **ORM**: SQLAlchemy 2.0 (mapped_column style)
- **DB**: PostgreSQL (JSONB, UUID, partial indexes)
- **Tests**: pytest (no DB required for classification tests)
- **No framework**: plain scripts + service classes

## Directory Layout

```
src/
  config.py                  # DATABASE_URL from env
  db/
    base.py                  # DeclarativeBase
    session.py               # engine + SessionLocal
    models/                  # Group, Member, Message, Topic, MessageTopic, ProfileSnapshot
  ingest/                    # Chat log parsers + IngestService (DO NOT MODIFY)
  classification/
    topic_rules.py           # Taxonomy: 8 TopicDefinition objects + confidence constants
    topic_classifier.py      # TopicClassifier.classify() → List[TopicMatch]
    classification_service.py # ClassificationService.run() → ClassificationResult
  processing/
    pipeline_types.py        # PipelineParams dataclass (all pipeline inputs)
    pipeline_result.py       # StageResult types + PipelineResult
    pipeline.py              # run_stage1_pipeline() – main orchestrator
    __init__.py              # re-exports public API
scripts/
  init_db.py                 # create_all()
  init_topics.py             # seed topics table from topic_rules.TOPICS
  import_chat.py             # CLI wrapper for IngestService
  run_topic_classification.py # CLI wrapper for ClassificationService
  run_stage1_pipeline.py     # Stage 1 full-chain CLI entry point
tests/
  test_classification.py     # 17 unit tests, no DB required
  test_ingest_service.py
  test_parsers.py
  test_pipeline.py           # 37 unit tests for processing layer, no DB required
```

## Running Tests

```bash
# From repo root
python -m pytest tests/test_classification.py -v
python -m pytest tests/ -v
```

No database connection needed for classification tests (all in-memory).

## Classification Module – Key Design Decisions

### Batch Pagination: Keyset (NOT OFFSET)

`ClassificationService._classify_messages()` uses **keyset pagination**
(`Message.id > last_seen_id`) instead of OFFSET.

**Why**: In incremental mode, the query filters out already-classified messages
via `NOT EXISTS`. After each batch is written, those rows become classified and
the result set shrinks. OFFSET on a shrinking set skips messages ("漏数" bug).
Keyset pagination is stable: we always advance the cursor to the last processed
`id`, so no message is ever skipped or double-processed.

### Incremental vs Rerun

- **Incremental** (`rerun=False`, default): only processes messages with no
  existing `MessageTopic` row for the current `classifier_version`.
- **Rerun** (`rerun=True`): deletes all existing rows for the version first,
  then re-classifies everything. Safe to re-run at any time.

### Primary Label Selection

1. Only topics with `is_primary_eligible=True` are candidates.
2. Highest `confidence` wins; ties broken by order in `TOPICS` list.
3. Fallback: if no eligible candidate exists, highest-confidence match overall
   wins (should not occur with current taxonomy – all 8 topics are eligible).

### Conflict Handling (Multi-Topic Messages)

A message can match multiple topics simultaneously (e.g. `casual_chat` + `meme`
both fire on "哈哈哈哈"). **All** matched topics are written to `message_topics`
so the full signal is preserved. `is_primary=True` is set on exactly one topic
per message (see selection logic above).

### Evidence Format (per MessageTopic row)

```json
{
  "rule_name": "keyword_match_v1",
  "text_source": "normalized_content",
  "matched_keywords": ["bug", "报错"],
  "strong_matched_keywords": ["bug"],
  "weak_matched_keywords": ["报错"],
  "matched_excerpt": "…这个 bug 怎么修？…"
}
```

`matched_excerpt` is a ≤63-char snippet centred on the first matched keyword,
padded with "…" when truncated. Useful for auditing classification decisions.

### ClassificationResult Fields

| Field | Meaning |
|---|---|
| `messages_processed` | Messages classified in this run |
| `messages_skipped_already_classified` | Messages that already had results before this run (incremental mode) |
| `messages_unmatched` | Processed but no topic matched |
| `topic_assignments_written` | Rows inserted into `message_topics` |
| `missing_topic_assignments` | Assignments dropped because `topic_key` not in DB |
| `missing_topic_keys` | List of missing keys (triggers a `warnings.warn`) |

### Missing Topic Keys

If `topic_rules.py` defines a key that isn't seeded in the `topics` table,
the assignment is dropped and counted in `missing_topic_assignments`. A
`warnings.warn` is emitted. Fix: run `python scripts/init_topics.py`.

## Taxonomy (8 Topics, rule_v1)

`casual_chat`, `technical`, `gaming`, `emotion`, `argument`, `planning`,
`meme`, `question` — all `is_primary_eligible=True`.

Confidence constants: `STRONG_BASE=0.85`, `WEAK_BASE=0.55`,
`EXTRA_KW_BONUS=0.05`, `MAX_CONFIDENCE=0.95`.

## What NOT to Do

- Do **not** modify the `ingest/` module.
- Do **not** change the core DB schema (models) unless absolutely necessary.
- Do **not** start implementing Persona Profile until classification is stable.
- Do **not** use OFFSET-based pagination anywhere in classification.

## Readiness for Persona Profile Phase

The classification module is ready when:
- [x] All 17 tests pass
- [x] Keyset pagination prevents message skipping
- [x] `messages_skipped_already_classified` is accurately reported
- [x] `missing_topic_assignments` warns on unseeded topics
- [x] `is_primary_eligible` is enforced in primary label selection
- [x] Evidence includes `matched_excerpt` for auditability
- [ ] Validated on a real dataset (requires DB + seeded data)

**Current status**: ✅ Ready to proceed to Persona Profile once real-data
validation is done.

---

## Profiling Module (Task 5)

### Directory

```
src/profiling/
  __init__.py            # re-exports ProfileBuilder, ProfileData, ProfileService, ProfilingResult
  profile_analyzers.py   # pure analysis functions (no DB)
  profile_builder.py     # ProfileData dataclass + ProfileBuilder (no DB)
  profile_service.py     # DB orchestration: load → build → write
scripts/
  run_profile_generation.py  # CLI entry point
tests/
  test_profiling.py      # 44 unit tests, no DB required
```

### Analysis Dimensions (profile_v1)

**stats** (raw numbers, stored in JSONB):
| Field | Description |
|---|---|
| `message_count` | Total messages in window |
| `avg_message_length` | Average character count |
| `top_keywords` | `[{word, count}]` top-10 tokens, stopwords removed |
| `topic_distribution` | `{topic_key: count}` primary-topic assignments only |
| `all_topics_distribution` | `{topic_key: count}` all matched topics |
| `active_hours` | `{"0": n, ..., "23": n}` message volume by hour |
| `interaction_top` | `[{member_id, display_name, count}]` reply-based, top-5 |

**traits** (derived labels, stored in JSONB):
| Field | Values |
|---|---|
| `dominant_topics` | Top-5 topic_keys by primary count |
| `verbosity_level` | `"terse"` (<20 chars) / `"moderate"` (20-80) / `"verbose"` (>80) |
| `style_hints` | `["emoji_user", "question_asker", "meme_lover", "tech_talker"]` |
| `activity_pattern` | `"morning"` / `"afternoon"` / `"evening"` / `"night"` / `"mixed"` |

### Idempotency / Rerun

- Unique constraint: `(member_id, profile_version, window_start, window_end)`
- Incremental (default): skip members who already have a snapshot
- Rerun (`rerun=True`): delete matching snapshots, then re-generate

### Interaction Top Strategy

1. Primary: use `reply_to_message_id` — look up the sender of the replied-to message
2. Fallback: empty list (adjacency-based approximation not implemented in v1)

### Keyword Extraction

- Split on whitespace + CJK/ASCII punctuation
- Filter: ASCII tokens < 3 chars dropped; CJK tokens < 2 chars dropped
- Minimal stopword sets (Chinese + English) applied
- No external NLP library required

### Running Profile Generation

```bash
# Window params are REQUIRED (idempotency: snapshot key includes window bounds)
python scripts/run_profiling.py \
    --profile-version profile_v1 \
    --window-start 2000-01-01T00:00:00Z \
    --window-end   2099-12-31T23:59:59Z

# Restrict to one group + explicit year window
python scripts/run_profiling.py \
    --group-id <uuid> \
    --profile-version profile_v1 \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Re-run (replace existing):
python scripts/run_profiling.py ... --rerun

# run_profile_generation.py is DEPRECATED – use run_profiling.py
```

### Next Steps for Persona Profile

- Validate on real dataset (requires DB + seeded topics + classified messages)
- Consider jieba for better Chinese tokenisation in `top_keywords`
- Add adjacency-based interaction fallback when `reply_to_message_id` is sparse
- Replace rule-template `persona_summary` with LLM-based generator (future)

---

## Profiling Module – Correctness Fixes (post-review)

### Fix 1: Per-member savepoint transaction isolation

**Problem**: All members shared one session/transaction. A `session.rollback()` on
member N rolled back all previously flushed snapshots from members 1…N-1.

**Fix**: Each member's write is wrapped in `session.begin_nested()` (SAVEPOINT).
- Success → savepoint released into outer transaction
- Failure → only that savepoint rolled back; outer transaction continues
- Outer transaction committed after the loop

`profiles_written` now always equals the actual rows in `profile_snapshots`.

### Fix 2: classifier_version bound to topic statistics

**Problem**: `_load_topic_rows()` had no `classifier_version` filter. Multiple
classifier runs for the same message polluted topic distributions.

**Fix**:
- `ProfileService.run()` accepts explicit `classifier_version` (default: `CLASSIFIER_VERSION`)
- `_load_topic_rows()` adds `MessageTopic.classifier_version == classifier_version` to the WHERE clause
- `classifier_version` is stored in `profile_snapshots.stats["classifier_version"]` for traceability
- CLI exposes `--classifier-version` flag

**Semantic separation**:
- `profile_version` → which profiling algorithm (stored in `profile_snapshots.profile_version`)
- `classifier_version` → which topic classifier's rows to consume (stored in `stats`)

### Fix 3: member_id / group_id consistency check

`ProfileService._assert_member_in_group()` raises `ValueError` if the loaded
member's `group_id` does not match the specified `group_id`. Called before any
writes when both `member_id` and `group_id` are provided.

### Fix 4: Missing topic mapping – no silent skip

`_load_topic_rows()` now returns `(rows, missing_count)`. Missing topic IDs
are counted, a `warnings.warn` is emitted per member, and `ProfilingResult`
exposes `missing_topic_count`. The CLI prints it with a remediation hint.

### Fix 5: Import coupling removed from `__init__.py`

`src/profiling/__init__.py` no longer imports `ProfileService` / `ProfilingResult`.
Pure modules (`profile_analyzers`, `profile_builder`) can be imported without
triggering DB engine initialisation. Import `ProfileService` directly:
```python
from src.profiling.profile_service import ProfileService, ProfilingResult
```

### Test coverage for fixes (75 total, all pass)

| Test class | What it verifies |
|---|---|
| `TestClassifierVersionInBuilder` | `classifier_version` stored in `stats`; different versions produce different profiles |
| `TestMemberGroupConsistency` | `_assert_member_in_group` raises on mismatch / missing member |
| `TestSavepointIsolationLogic` | Loop logic: failed member doesn't affect written count |
| `TestTopicRowsClassifierVersionFilter` | SQL WHERE includes `classifier_version` |
| `TestImportIsolation` | Pure modules importable without DB; `ProfileService` not in `__init__` |

---

## API Layer (Task 6)

### Directory

```
src/api/
  app.py            # FastAPI app, mounts all routers under /api/v1
  deps.py           # get_db() dependency: yields Session per request
  repository.py     # Read-only DB query functions (no business logic)
  routes/
    health.py       # GET /api/v1/health
    groups.py       # GET /api/v1/groups, /{group_id}, /{group_id}/members
    members.py      # GET /api/v1/members/{id}, /messages, /profile/latest, /profiles
  schemas/
    common.py       # PagedResponse[T] generic envelope
    group.py        # GroupSchema
    member.py       # MemberSchema
    message.py      # MessageSchema
    profile.py      # ProfileSnapshotSchema
scripts/
  run_api.py        # Launch script (uvicorn wrapper)
tests/
  test_api.py       # 32 tests, SQLite in-memory, no PostgreSQL required
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/health` | Liveness check |
| GET | `/api/v1/groups` | All groups + member_count + message_count |
| GET | `/api/v1/groups/{group_id}` | Single group by UUID |
| GET | `/api/v1/groups/{group_id}/members` | Members in group + latest_profile_snapshot_at |
| GET | `/api/v1/members/{member_id}` | Member + latest_profile_snapshot_at |
| GET | `/api/v1/members/{member_id}/messages` | Paged messages (newest first) |
| GET | `/api/v1/members/{member_id}/profile/latest` | Most recent profile snapshot |
| GET | `/api/v1/members/{member_id}/profiles` | Paged profile history |

### Pagination

- **Groups**: `limit` (default 100, max 500) + `offset`. Ordered `name ASC`. Returns `PagedResponse[GroupSchema]`.
- **Group Members**: `limit` (default 100, max 500) + `offset`. Ordered `display_name ASC`. Returns `PagedResponse[MemberSchema]`.
- **Messages**: `limit` (default 50, max 200) + `offset`. Ordered `sent_at DESC, id DESC`.
  - Optional filters: `sent_at_gte`, `sent_at_lte` (ISO-8601 UTC).
  - Cross-field validation: `sent_at_gte > sent_at_lte` → HTTP 422.
- **Profiles**: `limit` (default 20, max 100) + `offset`. Ordered `snapshot_at DESC, id DESC` (stable).
  - Optional filter: `profile_version` (exact match).
- All paged responses use `PagedResponse[T]` envelope: `{items, total, limit, offset}`.

### Error Handling

- 404 for unknown `group_id` / `member_id` / no profile snapshot.
- 422 (FastAPI/Pydantic) for invalid query params (e.g. `limit=0`, `limit=9999`).
- 422 for cross-field violations (e.g. `sent_at_gte > sent_at_lte`).
- No global exception handler — FastAPI defaults are sufficient for MVP.

### Starting the API

```bash
# Development (auto-reload)
python scripts/run_api.py --reload

# Production-style
python scripts/run_api.py --host 127.0.0.1 --port 8000

# Or directly:
uvicorn src.api.app:app --host 0.0.0.0 --port 8000

# Interactive docs:
# http://localhost:8000/docs   (Swagger UI)
# http://localhost:8000/redoc  (ReDoc)
```

### Design Constraints

- **Read-only**: no POST/PUT/DELETE endpoints.
- **No auth**: MVP only — add middleware later if needed.
- **Low coupling**: `src/api/` depends only on `src/db/` (models + session).
  It does NOT import from `ingest/`, `classification/`, or `profiling/`.
- **Test isolation**: `StaticPool` + SQLite DDL (not `create_all`) avoids
  PostgreSQL-specific type errors (JSONB/UUID) in tests.

### Known Limitation

- `test_ingest_service.py::test_txt_without_external_id_allows_duplicates`
  was already failing before Task 6 (pre-existing ingest bug, not introduced here).
  All 32 API tests pass; 129/130 total tests pass.

### Dependencies

- `requirements.txt`: runtime deps (fastapi, uvicorn, sqlalchemy, pydantic, python-dotenv, psycopg2-binary)
- `requirements-dev.txt`: `-r requirements.txt` + pytest, httpx, anyio
- Install: `pip install -r requirements-dev.txt`

### limit cap — Authority Layer

- **Route layer** (FastAPI `Query(ge=1, le=MAX)`) is the **authority**: returns HTTP 422 for out-of-range values before the request reaches the repository.
- **Repository layer** also clamps with `min(max(1, limit), MAX)` as a safety net for direct programmatic calls.
- No logic conflict: route rejects bad values early; repository never sees them in normal API flow.

---

## Processing Module – Stage 1 Orchestration (Task 7)

### Directory

```
src/processing/
  __init__.py          # re-exports PipelineParams, PipelineResult, run_stage1_pipeline
  pipeline_types.py    # PipelineParams dataclass (all pipeline inputs)
  pipeline_result.py   # IngestStageResult, TopicsInitStageResult,
                       # ClassificationStageResult, ProfilingStageResult, PipelineResult
  pipeline.py          # run_stage1_pipeline() + private stage runners
scripts/
  run_stage1_pipeline.py  # CLI entry point for the full Stage 1 chain
tests/
  test_pipeline.py     # 37 unit tests, no DB required
```

### Stage Sequence

```
Step 1: ingest          → IngestService.ingest_file()
Step 2: topics_init     → idempotent DB seed (inline, same logic as init_topics.py)
Step 3: classification  → ClassificationService.run()
Step 4: profiling       → ProfileService.run()
```

### Key Design Decisions

**Error-handling strategy**: Default is **abort on first failure** (`continue_on_error=False`).
Remaining stages are recorded as `"skipped"` with an explanatory message.
Set `continue_on_error=True` to attempt all stages regardless; `overall_status` becomes `"partial"`.

**Topics init is automatic**: `topics_init` always runs before classification (unless
classification is skipped). It is idempotent – existing topics are left untouched.
Users never need to manually run `init_topics.py` before the pipeline.

**topics_init failure aborts classification**: If topics can't be seeded, classification
would fail anyway (no topic_id map). The abort propagates naturally.

**Dry-run mode**: `dry_run=True` prints what would execute without touching the DB.
Returns `overall_status="dry_run"`.

**Session safety in topics_init**: `session = None` before the try block; `SessionLocal()`
is called inside the try so DB connection errors are caught and returned as `StageResult(status="failed")`.

### PipelineParams Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `input_file` | `Optional[Path]` | `None` | Chat log file (.json/.csv/.txt) |
| `platform` | `Optional[str]` | `None` | Platform hint (qq/wechat/discord/generic) |
| `group_name` | `Optional[str]` | `None` | Group name override (required for .txt) |
| `classifier_version` | `str` | `CLASSIFIER_VERSION` | Classification version tag |
| `rerun_classification` | `bool` | `False` | Delete + redo classification |
| `profile_version` | `str` | `PROFILE_VERSION` | Profiling algorithm version |
| `window_start` | `Optional[datetime]` | `None` | Required for profiling |
| `window_end` | `Optional[datetime]` | `None` | Required for profiling |
| `rerun_profiling` | `bool` | `False` | Delete + redo profiling |
| `group_id` | `Optional[UUID]` | `None` | Restrict classification + profiling |
| `member_id` | `Optional[UUID]` | `None` | Restrict profiling (requires group_id) |
| `skip_ingest` | `bool` | `False` | Skip ingest stage |
| `skip_classification` | `bool` | `False` | Skip classification stage |
| `skip_profiling` | `bool` | `False` | Skip profiling stage |
| `dry_run` | `bool` | `False` | Print plan, don't execute |
| `continue_on_error` | `bool` | `False` | Continue after stage failure |

### PipelineResult.overall_status Values

| Value | Meaning |
|---|---|
| `"success"` | All non-skipped stages completed without error |
| `"partial"` | At least one stage failed; `continue_on_error=True` allowed others to run |
| `"failed"` | At least one stage failed; pipeline was aborted |
| `"dry_run"` | `dry_run=True`; no stages executed |

### Exit Codes (run_stage1_pipeline.py)

| Code | Meaning |
|---|---|
| `0` | success or dry_run |
| `1` | failed |
| `2` | partial (some stages failed, continue_on_error=True) |

### Running the Pipeline

```bash
# Full run
python scripts/run_stage1_pipeline.py \
    --input-file data/raw/chat.json \
    --platform qq \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Skip ingest (data already in DB), rerun classification + profiling
python scripts/run_stage1_pipeline.py \
    --skip-ingest \
    --rerun-classification --rerun-profiling \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Dry run
python scripts/run_stage1_pipeline.py \
    --input-file data/raw/chat.json --platform qq \
    --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z \
    --dry-run

# Restrict to one group, continue even if a stage fails
python scripts/run_stage1_pipeline.py \
    --skip-ingest \
    --group-id <uuid> \
    --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z \
    --continue-on-error
```

### Test Count (Task 7)

- 37 new tests in `tests/test_pipeline.py` (no DB required)
- Total: 166 pass, 1 pre-existing failure (ingest TXT dedup bug, unchanged)

### Known Limitations / Future Work

- Pipeline is **serial** (single-machine, single-process). Replace `run_stage1_pipeline()`
  with a task-queue dispatcher (Celery, etc.) when background execution is needed.
- No persistent run log. Add a `pipeline_runs` table if audit history is required.
- `topics_init` is inlined in `pipeline.py`. If the taxonomy grows complex, extract
  to `src/processing/topic_init_service.py`.
- `window_start` / `window_end` are required for profiling even in `--skip-profiling`
  mode when called via the library API (only the CLI enforces the skip guard).

