# OneBot v11 Fixtures (NapCat / QQNT)

This directory contains fake OneBot v11 raw event fixtures derived from real
NapCat (QQNT) field discovery. Use these as reference structures when building
the QQ Collector adapter.

## Source

All fixtures are based on Field Discovery samples M-01 through M-08,
collected via NapCat v4.18.6 + QQNT v9.9.31 over OneBot v11 WebSocket.
The original redacted samples live in `D:\NFA-qq-field-discovery\samples\`.

## Fixture Mapping

| Fixture File | Source Sample | Event Type | Key Features |
|---|---|---|---|
| `group_text.json` | M-01 | message | Plain text from group owner |
| `group_text_member.json` | M-02 | message | Plain text from regular member (role=member) |
| `group_text_card.json` | M-03 | message | Sender with group card set |
| `group_text_emoji.json` | M-04 | message | Text with Unicode emoji |
| `group_text_at.json` | M-05 | message | Multi-segment: text + at + text |
| `group_image.json` | M-06 | message | Image segment with caption text |
| `group_reply.json` | M-07 | message | Reply (quote) + at + text |
| `group_recall_notice.json` | M-08 | notice | group_recall notice event |

## Field Type Notes (from real NapCat)

These are important gotchas discovered during field discovery:

- **`real_seq`** is a **STRING**, not an integer.
- **`at.data.qq`** is a **STRING** (e.g. `"10006"`), not an integer.
- **`reply.data.id`** is a **STRING** (e.g. `"90001"`), not an integer.
- **`sender.card`** can be an **empty string** `""`, not null.
- **`image.data.file_size`** can be a **STRING** in some cases.
- **`image.data.file_id`** does NOT exist in NapCat output.
- **`font`** is always present (value `14`).
- **`message_format`** is always `"array"` with NapCat.
- **`message_seq`** and **`real_id`** are integers.
- Notice events (M-08) have a **completely different** top-level structure
  from message events — no `sender`, `message`, `raw_message`, etc.

## Notice vs Message

The `group_recall_notice.json` fixture demonstrates that OneBot v11 **notice**
events share almost no fields with **message** events:

**Message event fields:**
`self_id`, `user_id`, `time`, `message_id`, `message_seq`, `real_id`,
`real_seq`, `message_type`, `sender`, `raw_message`, `font`, `sub_type`,
`message`, `message_format`, `post_type`, `group_id`, `group_name`

**Notice event fields (group_recall):**
`time`, `self_id`, `post_type`, `group_id`, `user_id`,
`notice_type`, `operator_id`, `message_id`

## Fixture Convention

- Each file is a single JSON object (one event).
- `_comment` and `_source` fields are metadata and should be ignored by code.
- All IDs use placeholder numeric values (10001, 10002, etc.) — no real QQ numbers.
- Image URLs use `example.com` — no real multimedia URLs.
- These fixtures are **not** for ingestion into the database — they represent
  raw OneBot v11 wire format for adapter development and testing.
