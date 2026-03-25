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
scripts/
  init_db.py                 # create_all()
  init_topics.py             # seed topics table from topic_rules.TOPICS
  import_chat.py             # CLI wrapper for IngestService
  run_topic_classification.py # CLI wrapper for ClassificationService
tests/
  test_classification.py     # 17 unit tests, no DB required
  test_ingest_service.py
  test_parsers.py
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
python scripts/run_profile_generation.py \
    --group-id <uuid> \
    --profile-version profile_v1 \
    --window-start 2026-01-01T00:00:00Z \
    --window-end   2026-12-31T23:59:59Z

# Re-run (replace existing):
python scripts/run_profile_generation.py ... --rerun
```

### Next Steps for Persona Profile

- Validate on real dataset (requires DB + seeded topics + classified messages)
- Consider jieba for better Chinese tokenisation in `top_keywords`
- Add adjacency-based interaction fallback when `reply_to_message_id` is sparse
- Replace rule-template `persona_summary` with LLM-based generator (future)
