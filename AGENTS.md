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
    models/                  # Group, Member, Message, Topic, MessageTopic, ProfileSnapshot, LegendMember
  ingest/                    # Chat log parsers + IngestService (DO NOT MODIFY)
  classification/
    topic_rules.py           # Taxonomy: 8 TopicDefinition objects + confidence constants
    topic_classifier.py      # TopicClassifier.classify() → List[TopicMatch]
    classification_service.py # ClassificationService.run() → ClassificationResult
    topic_service.py         # init_topics() – single canonical topic-init entry point
  profiling/
    profile_analyzers.py     # pure analysis functions
    profile_builder.py       # ProfileData + ProfileBuilder
    profile_service.py       # DB orchestration
  legend/
    __init__.py              # no re-exports (avoids DB side-effects on import)
    archive_policy.py        # pure eligibility + state-transition guards
    legend_repository.py     # DB reads/writes for legend_members
    legend_schemas.py        # ArchiveResult, RestoreResult, SimulationToggleResult, LegendMemberSchema
    legend_service.py        # LegendService: archive/restore/enable-sim/disable-sim/list
  api/
    app.py                   # FastAPI app (v0.2.0), mounts all routers
    deps.py                  # get_db() dependency
    repository.py            # read-only query functions
    routes/
      health.py              # GET /api/v1/health
      groups.py              # GET /api/v1/groups, /{group_id}, /{group_id}/members
      members.py             # GET /api/v1/members/{id}, /messages, /profile/latest, /profiles
      legend.py              # Legend Archive endpoints (GET + POST)
    schemas/
      common.py              # PagedResponse[T]
      group.py / member.py / message.py / profile.py / (legend in legend_schemas.py)
  processing/
    pipeline.py              # PipelineParams, StageOutcome, PipelineResult, run_stage1_pipeline()
scripts/
  init_db.py                 # create_all()
  init_topics.py             # thin CLI wrapper → topic_service.init_topics()
  import_chat.py             # CLI wrapper for IngestService
  run_topic_classification.py # CLI wrapper for ClassificationService
  run_profiling.py           # CLI wrapper for ProfileService
  run_pipeline.py            # unified Stage-1 pipeline CLI
  archive_member.py          # archive a member into Legend Archive
  list_legend_members.py     # list legend members with filters
  toggle_legend_simulation.py # enable/disable simulation for a legend member
  restore_legend_member.py   # restore an archived legend member
tests/
  test_classification.py     # 17 unit tests, no DB required
  test_ingest_service.py
  test_parsers.py
  test_profiling.py          # 75 unit tests, no DB required
  test_api.py                # 32 API tests, SQLite in-memory
  test_processing.py         # 29 unit tests, no DB required
  test_legend.py             # 46 unit tests (policy + service + API), SQLite in-memory
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

## Processing Module – Stage-1 Pipeline (Task 7)

### Directory

```
src/
  classification/
    topic_service.py   # ← canonical topic init function (single implementation)
  processing/
    __init__.py        # minimal, no DB side-effects on import
    pipeline.py        # PipelineParams, PipelineResult, run_stage1_pipeline()
scripts/
  run_pipeline.py      # CLI entry point for the full pipeline
tests/
  test_processing.py   # 29 unit tests, no DB required
```

### Pipeline Stages (in order)

| Stage | Key | What it does |
|---|---|---|
| ingest | `ingest` | Parse chat file → write messages to DB |
| topics_init | `topics_init` | Seed topics table (idempotent) |
| classification | `classification` | Classify messages by topic |
| profiling | `profiling` | Build Persona Profile snapshots |

### Topic Init – Single Canonical Implementation

**Authority**: `src/classification/topic_service.init_topics()`

Both callers delegate to this one function:
- `scripts/init_topics.py` → thin CLI wrapper, calls `init_topics()`
- `src/processing/pipeline._run_topics_init()` → calls `init_topics()`

There is **no** second implementation of topic seeding logic anywhere.
`init_topics()` accepts an optional `session` parameter; if `None`, it creates,
commits, and closes its own `SessionLocal()`.

### PipelineParams Cross-field Validation

`_validate_params()` is called at the very start of `run_stage1_pipeline()`,
before any DB work:

| Check | Error message |
|---|---|
| `member_id` without `group_id` | "member_id requires group_id" |
| profiling stage runs but `window_start` or `window_end` is None | "window_start and window_end are required for the profiling stage" |
| `window_start >= window_end` | "window_start … must be before window_end" |
| ingest stage runs but `chat_file` is None | "chat_file is required for the ingest stage" |
| unknown stage name in `skip_stages` | "Unknown stage name(s) in skip_stages" |

### run_id Format

`YYYYMMDDTHHMMSS_<6-char hex>` (UTC timestamp + random suffix from `uuid4().hex[:6]`).

Rationale: timestamp gives human-readable ordering; 6-char hex suffix prevents
collisions when two runs start within the same second (probability ≈ 1/16M).

### Error Handling – Result Object vs Logs

- `StageOutcome.error_summary`: short one-line string (`"RuntimeError: …"`).
  No traceback. Safe to surface in APIs or task-system UIs.
- Full traceback: emitted via `logging.error()` only. Never stored in result objects.
- Pipeline does **not** abort on stage failure: all stages are attempted in order.
  `PipelineResult.success` is `False` if any stage failed.

### Running the Pipeline

```bash
# Full pipeline
python scripts/run_pipeline.py \
    --chat-file exports/chat.json \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Skip ingest (data already in DB)
python scripts/run_pipeline.py \
    --skip ingest \
    --window-start 2000-01-01T00:00:00Z \
    --window-end   2099-12-31T23:59:59Z

# Re-run classification + profiling from scratch
python scripts/run_pipeline.py \
    --skip ingest topics_init \
    --rerun \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Verbose logging
python scripts/run_pipeline.py --skip ingest --log-level INFO \
    --window-start 2000-01-01T00:00:00Z --window-end 2099-12-31T23:59:59Z
```

### Stage-1 Stability Checklist

- [x] ingest → topics_init → classification → profiling order enforced
- [x] Each stage independently skippable via `--skip`
- [x] `--rerun` passed through to classification and profiling services
- [x] Topic init has single canonical implementation (`topic_service.init_topics`)
- [x] Cross-field param validation before any DB work
- [x] `run_id` collision-resistant (timestamp + random suffix)
- [x] `error_summary` in result objects; traceback in logs only
- [x] 29 unit tests pass (no DB required)
- [x] Full test suite: 158 pass, 1 pre-existing failure (ingest TXT dedup bug)

**Current status**: ✅ Stage-1 pipeline is stable and closed-loop.

---

## Legend Archive Module (Phase 2, Task 1)

### Directory

```
src/legend/
  __init__.py          # no re-exports (pure modules importable without DB)
  archive_policy.py    # pure eligibility + state-transition guards (no DB)
  legend_repository.py # DB reads/writes for legend_members
  legend_schemas.py    # ArchiveResult, RestoreResult, SimulationToggleResult, LegendMemberSchema
  legend_service.py    # LegendService: archive/restore/enable-sim/disable-sim/list
scripts/
  archive_member.py          # archive a member
  list_legend_members.py     # list with filters
  toggle_legend_simulation.py # enable/disable simulation
  restore_legend_member.py   # restore an archived member
tests/
  test_legend.py       # 46 unit tests (policy + service + API), SQLite in-memory
```

### legend_members Table

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `member_id` | UUID FK → members.id | UNIQUE – one row per member |
| `group_id` | UUID FK → groups.id | |
| `archive_status` | String(32) | `"archived"` / `"restored"` |
| `archived_at` | DateTime(tz) | NOT NULL |
| `archived_reason` | Text | nullable |
| `archived_by` | String(255) | nullable |
| `source_profile_snapshot_id` | UUID FK → profile_snapshots.id | nullable; SET NULL on delete |
| `member_display_name_snapshot` | String(255) | denormalised identity |
| `member_external_id_snapshot` | String(255) | nullable |
| `member_status_snapshot` | String(32) | nullable |
| `simulation_enabled` | Boolean | default False |
| `created_at` / `updated_at` | DateTime(tz) | server_default + explicit on insert |

Indexes: `(group_id, archive_status, archived_at)`, `(simulation_enabled, archive_status)`, `(source_profile_snapshot_id)`.

### Business Rules

**Eligibility**: default `member.status == "left"`; `force=True` bypasses.

**Idempotency**: if already `archived`, returns existing record with `was_already_archived=True`.

**Re-archive after restore**: updates the existing row in place (same PK); refreshes all archive fields and resets `simulation_enabled=False`.

**Restore**: sets `archive_status="restored"`, `simulation_enabled=False`. Row and `members` row both preserved.

**Simulation toggle**: `enable_simulation` only allowed when `archive_status=="archived"`. `disable_simulation` always allowed.

**No profile snapshot**: stored as `source_profile_snapshot_id=None`. Archive still succeeds; caller should note this in `archived_reason`.

### API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/legend/members` | List (paged, filterable by group_id / archive_status / simulation_enabled) |
| GET | `/api/v1/legend/members/{member_id}` | Single legend member |
| POST | `/api/v1/legend/members/{member_id}/archive` | Archive (body: `archived_reason`, `archived_by`, `force`) |
| POST | `/api/v1/legend/members/{member_id}/restore` | Restore |
| POST | `/api/v1/legend/members/{member_id}/enable-simulation` | Enable simulation gate |
| POST | `/api/v1/legend/members/{member_id}/disable-simulation` | Disable simulation gate |

### CLI Usage

```bash
# Archive a member who has left
python scripts/archive_member.py <member_id> --reason "Left 2026-06-01" --by admin

# Force-archive regardless of status
python scripts/archive_member.py <member_id> --force

# List all archived members in a group
python scripts/list_legend_members.py --group-id <group_id> --status archived

# Enable simulation
python scripts/toggle_legend_simulation.py <member_id> enable

# Restore
python scripts/restore_legend_member.py <member_id>
```

### Test Isolation Note

`test_legend.py` sets `app.dependency_overrides[get_db]` inside the `setup_db`
fixture (not at module level) and restores the previous override on teardown.
This prevents cross-module contamination when running the full test suite.
`test_api.py` includes `legend_members` DDL so its engine is compatible if
the override is active.

### Current Status

- [x] `legend_members` model with all required fields and indexes
- [x] `archive_policy.py` pure guards (no DB)
- [x] `LegendService` with archive / restore / simulation toggle / list
- [x] Idempotent archive; re-archive after restore reuses same row
- [x] Profile snapshot anchor (None if member was never profiled)
- [x] 4 CLI scripts
- [x] 6 API endpoints
- [x] 46 unit tests pass; full suite 204 pass, 1 pre-existing failure

**Current status**: ✅ Legend Archive MVP complete. Ready for Persona Simulation phase.
