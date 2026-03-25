"""
Pure analysis functions for Persona Profile generation (v1).

All functions in this module are stateless and DB-free.  They receive
pre-loaded Python objects (lists of dicts / dataclass instances) and return
plain Python values, making them straightforward to unit-test.

Analysis dimensions
-------------------
1. message_stats       – count and average length
2. top_keywords        – frequency-ranked tokens from normalized_content
3. topic_distribution  – primary-topic counts + all-topic counts
4. active_hours        – message volume by hour-of-day (0-23)
5. interaction_top     – most-replied-to members (reply graph or adjacency)
6. traits              – derived labels (verbosity, activity pattern, style hints)
7. persona_summary     – rule-template text summary
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Minimal stopword sets (Chinese + English)
# ---------------------------------------------------------------------------

_ZH_STOPWORDS: frozenset = frozenset({
    "的", "了", "是", "在", "我", "你", "他", "她", "它", "们",
    "这", "那", "有", "和", "就", "不", "也", "都", "而", "及",
    "与", "或", "一", "个", "上", "下", "来", "去", "到", "说",
    "要", "会", "可", "对", "里", "后", "以", "为", "被", "所",
    "从", "之", "其", "但", "如", "还", "把", "让", "因", "用",
    "时", "当", "没", "很", "更", "最", "已", "将", "又", "再",
    "只", "才", "着", "过", "给", "向", "于", "比", "等", "中",
    "大", "小", "多", "少", "好", "坏", "新", "旧", "高", "低",
    "长", "短", "快", "慢", "热", "冷", "真", "假", "全", "半",
    "各", "每", "某", "另", "嗯", "哦", "啊", "哈", "呢", "吧",
    "呀", "哇", "哎", "唉", "哟", "喔", "嘿", "哼", "嗯嗯", "哦哦",
})

_EN_STOPWORDS: frozenset = frozenset({
    "i", "me", "my", "we", "our", "you", "your", "he", "she", "it",
    "they", "their", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "a", "an", "the", "and",
    "but", "or", "nor", "for", "yet", "so", "at", "by", "in", "of",
    "on", "to", "up", "as", "if", "than", "that", "this", "these",
    "those", "with", "from", "into", "not", "no", "just", "very",
    "too", "also", "then", "when", "where", "how", "what", "who",
    "ok", "okay", "yeah", "yes", "lol", "haha",
})

# Tokenisation: split on whitespace + common punctuation
_TOKEN_SPLIT_RE = re.compile(r"[\s\u3000\uff0c\u3002\uff01\uff1f\u300c\u300d\u201c\u201d\u2018\u2019,\.!?;:\"'()\[\]{}<>@#$%^&*+=|\\~/`]+")


# ---------------------------------------------------------------------------
# 1. Message stats
# ---------------------------------------------------------------------------

def compute_message_stats(messages: List[Any]) -> Dict[str, Any]:
    """
    Return basic volume and length statistics.

    Args:
        messages: list of Message ORM objects (or any object with .content /
                  .normalized_content attributes).

    Returns:
        {
            "message_count": int,
            "avg_message_length": float,   # characters
            "total_chars": int,
        }
    """
    count = len(messages)
    if count == 0:
        return {"message_count": 0, "avg_message_length": 0.0, "total_chars": 0}

    total_chars = sum(
        len(getattr(m, "normalized_content", None) or getattr(m, "content", "") or "")
        for m in messages
    )
    return {
        "message_count": count,
        "avg_message_length": round(total_chars / count, 2),
        "total_chars": total_chars,
    }


# ---------------------------------------------------------------------------
# 2. Top keywords
# ---------------------------------------------------------------------------

def compute_top_keywords(messages: List[Any], top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Extract the most frequent tokens from message text.

    Tokenisation strategy (minimal, no NLP library required):
    - Use normalized_content when available, fall back to content.
    - Split on whitespace and common punctuation.
    - Keep tokens with length >= 2 (Chinese) or >= 3 (ASCII-only).
    - Drop tokens in the stopword sets.
    - Count and return top_n.

    Returns:
        [{"word": str, "count": int}, ...]  sorted by count descending.
    """
    counter: Counter = Counter()

    for m in messages:
        text = getattr(m, "normalized_content", None) or getattr(m, "content", "") or ""
        if not text:
            continue
        tokens = _TOKEN_SPLIT_RE.split(text.strip())
        for tok in tokens:
            tok = tok.strip()
            if not tok:
                continue
            tok_lower = tok.lower()
            # Length filter: ≥2 for CJK-containing, ≥3 for pure ASCII
            is_ascii = tok.isascii()
            if is_ascii and len(tok) < 3:
                continue
            if not is_ascii and len(tok) < 2:
                continue
            # Stopword filter
            if tok_lower in _EN_STOPWORDS or tok in _ZH_STOPWORDS:
                continue
            counter[tok] += 1

    return [{"word": w, "count": c} for w, c in counter.most_common(top_n)]


# ---------------------------------------------------------------------------
# 3. Topic distribution
# ---------------------------------------------------------------------------

def compute_topic_distribution(
    topic_rows: List[Any],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Aggregate topic counts from MessageTopic rows.

    Args:
        topic_rows: list of objects with attributes:
                    .topic_key (str), .is_primary (bool)

    Returns:
        (primary_dist, all_dist)
        primary_dist: {topic_key: count}  – only is_primary=True rows
        all_dist:     {topic_key: count}  – all matched rows
    """
    primary_dist: Dict[str, int] = {}
    all_dist: Dict[str, int] = {}

    for row in topic_rows:
        key = row.topic_key
        all_dist[key] = all_dist.get(key, 0) + 1
        if row.is_primary:
            primary_dist[key] = primary_dist.get(key, 0) + 1

    return primary_dist, all_dist


# ---------------------------------------------------------------------------
# 4. Active hours
# ---------------------------------------------------------------------------

def compute_active_hours(messages: List[Any]) -> Dict[str, int]:
    """
    Count messages per hour-of-day (0-23) using sent_at in UTC.

    Time-zone note
    --------------
    All ``sent_at`` values are stored as UTC in the database (the ingest
    module normalises timestamps to UTC before writing).  This function
    reads ``sent_at.hour`` directly, which is the UTC hour.  No additional
    conversion is performed.  Callers that want local-time bucketing must
    convert ``sent_at`` before passing messages to this function.

    Returns:
        {"0": int, "1": int, ..., "23": int}
        Hours with zero messages are omitted.
    """
    counter: Counter = Counter()
    for m in messages:
        sent_at = getattr(m, "sent_at", None)
        if sent_at is not None:
            counter[str(sent_at.hour)] += 1
    return dict(counter)


# ---------------------------------------------------------------------------
# 5. Interaction top (reply-based)
# ---------------------------------------------------------------------------

def compute_interaction_top(
    reply_targets: List[Dict[str, Any]],
    top_n: int = 5,
) -> List[Dict[str, Any]]:
    """
    Rank interaction partners by reply frequency.

    Args:
        reply_targets: pre-resolved list of dicts, one per outgoing reply:
                       {"member_id": str, "display_name": str}
                       (built by ProfileService from reply_to_message_id lookups)

    Returns:
        [{"member_id": str, "display_name": str, "count": int}, ...]
        sorted by count descending, up to top_n entries.
    """
    counter: Counter = Counter()
    name_map: Dict[str, str] = {}

    for entry in reply_targets:
        mid = str(entry["member_id"])
        counter[mid] += 1
        name_map[mid] = entry.get("display_name", mid)

    return [
        {"member_id": mid, "display_name": name_map[mid], "count": cnt}
        for mid, cnt in counter.most_common(top_n)
    ]


# ---------------------------------------------------------------------------
# 6. Derived traits
# ---------------------------------------------------------------------------

_VERBOSITY_THRESHOLDS = (20, 80)   # (terse→moderate, moderate→verbose)

_HOUR_BANDS = {
    "morning":   frozenset(range(6, 12)),
    "afternoon": frozenset(range(12, 18)),
    "evening":   frozenset(range(18, 22)),
    "night":     frozenset(range(22, 24)) | frozenset(range(0, 6)),
}


def compute_verbosity_level(avg_length: float) -> str:
    """Return 'terse' | 'moderate' | 'verbose'."""
    lo, hi = _VERBOSITY_THRESHOLDS
    if avg_length < lo:
        return "terse"
    if avg_length <= hi:
        return "moderate"
    return "verbose"


def compute_activity_pattern(active_hours: Dict[str, int]) -> str:
    """
    Determine the dominant activity band from hourly counts.

    Returns 'morning' | 'afternoon' | 'evening' | 'night' | 'mixed'.
    'mixed' is returned when no single band accounts for ≥40 % of messages.
    """
    if not active_hours:
        return "mixed"

    band_totals: Dict[str, int] = {band: 0 for band in _HOUR_BANDS}
    total = 0
    for hour_str, cnt in active_hours.items():
        hour = int(hour_str)
        total += cnt
        for band, hours in _HOUR_BANDS.items():
            if hour in hours:
                band_totals[band] += cnt

    if total == 0:
        return "mixed"

    dominant_band = max(band_totals, key=lambda b: band_totals[b])
    if band_totals[dominant_band] / total >= 0.40:
        return dominant_band
    return "mixed"


def compute_style_hints(
    messages: List[Any],
    topic_dist: Dict[str, int],
    avg_length: float,
) -> List[str]:
    """
    Derive a small set of style labels from message content and topic data.

    Possible hints:
    - "emoji_user"       – >10 % of messages contain an emoji
    - "question_asker"   – question topic is in top-3 primary topics
    - "meme_lover"       – meme topic is in top-3 primary topics
    - "tech_talker"      – technical topic is in top-3 primary topics
    - "night_owl"        – activity_pattern == 'night'  (caller passes this)
    """
    hints: List[str] = []

    # Emoji usage
    emoji_re = re.compile(
        "[\U0001F300-\U0001F9FF"
        "\U00002600-\U000027BF"
        "\U0001FA00-\U0001FA9F"
        "\U00002702-\U000027B0]+",
        re.UNICODE,
    )
    emoji_count = sum(
        1 for m in messages
        if emoji_re.search(getattr(m, "normalized_content", None) or getattr(m, "content", "") or "")
    )
    if messages and emoji_count / len(messages) > 0.10:
        hints.append("emoji_user")

    # Topic-based hints
    top3 = sorted(topic_dist, key=lambda k: topic_dist[k], reverse=True)[:3]
    if "question" in top3:
        hints.append("question_asker")
    if "meme" in top3:
        hints.append("meme_lover")
    if "technical" in top3:
        hints.append("tech_talker")

    return hints


# ---------------------------------------------------------------------------
# 7. Persona summary (rule-template)
# ---------------------------------------------------------------------------

_TOPIC_ZH: Dict[str, str] = {
    "casual_chat": "日常闲聊",
    "technical":   "技术讨论",
    "gaming":      "游戏",
    "emotion":     "情感表达",
    "argument":    "争论/辩论",
    "planning":    "计划安排",
    "meme":        "表情包/梗",
    "question":    "提问求助",
}

_ACTIVITY_ZH: Dict[str, str] = {
    "morning":   "上午活跃度较高",
    "afternoon": "下午活跃度较高",
    "evening":   "晚间活跃度较高",
    "night":     "夜间活跃度较高",
    "mixed":     "全天活跃时间较为分散",
}

_VERBOSITY_ZH: Dict[str, str] = {
    "terse":    "发言偏短",
    "moderate": "发言长度适中",
    "verbose":  "发言偏长",
}


def _activity_level_zh(message_count: int) -> str:
    if message_count >= 200:
        return "非常活跃"
    if message_count >= 50:
        return "较为活跃"
    if message_count >= 10:
        return "偶尔发言"
    return "发言较少"


def build_persona_summary(
    message_count: int,
    dominant_topics: List[str],
    verbosity_level: str,
    activity_pattern: str,
    interaction_top: List[Dict[str, Any]],
    window_start: Any,
    window_end: Any,
) -> str:
    """
    Generate a readable Chinese summary from structured statistics.

    All inputs come from previously computed stats/traits — nothing is
    invented.  The template is intentionally simple so it can be replaced
    by an LLM-based generator in a future version.
    """
    activity_level = _activity_level_zh(message_count)

    # Topic phrase
    top_topic_keys = dominant_topics[:3]
    if top_topic_keys:
        topic_names = [_TOPIC_ZH.get(k, k) for k in top_topic_keys]
        topic_phrase = "、".join(topic_names)
        topic_sentence = f"主要参与{topic_phrase}相关话题"
    else:
        topic_sentence = "暂无明显话题偏好"

    verbosity_phrase = _VERBOSITY_ZH.get(verbosity_level, "发言长度适中")
    time_phrase = _ACTIVITY_ZH.get(activity_pattern, "全天活跃时间较为分散")

    # Interaction note
    if interaction_top:
        top_names = [e["display_name"] for e in interaction_top[:2]]
        interaction_note = f"常与 {'、'.join(top_names)} 形成互动。"
    else:
        interaction_note = ""

    # Window description
    try:
        ws = window_start.strftime("%Y-%m-%d")
        we = window_end.strftime("%Y-%m-%d")
        window_note = f"（统计窗口：{ws} 至 {we}）"
    except Exception:
        window_note = ""

    parts = [
        f"该成员在当前时间窗口内{activity_level}，",
        f"{topic_sentence}，",
        f"{verbosity_phrase}，",
        f"{time_phrase}。",
    ]
    if interaction_note:
        parts.append(interaction_note)
    if window_note:
        parts.append(window_note)

    return "".join(parts)
