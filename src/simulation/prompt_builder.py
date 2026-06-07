"""
Prompt builder – pure function, no DB, no LLM.

Takes pre-loaded data (legend_member fields, profile_snapshot fields,
sampled messages, current message) and assembles a prompt_messages list
in the standard  [{"role": ..., "content": ...}]  format.

Mandatory prompt boundaries (enforced in every call):
  1. Explicit declaration: this is a simulation, not the real person.
  2. Constraint: do not fabricate facts absent from the historical record.
  3. Constraint: express uncertainty rather than forcing a persona when
     evidence is insufficient.
  4. Style focus: mirror language style, topic tendencies, expression habits.
  5. The model must NOT claim to be the actual member.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.simulation.simulation_schemas import HistoricalMessage, SimulationInput

# Hard boundaries embedded in every system prompt – never alter wording without review.
_SIMULATION_DISCLAIMER = (
    "【重要声明】你正在扮演一个基于历史归档数据构建的人格模拟角色。"
    "这不是真实成员本人，不代表其真实观点或当前状态。"
    "你不得声称自己就是该真实成员。"
)

_NO_FABRICATION_CONSTRAINT = (
    "【约束】请严格基于以下提供的历史资料进行回应。"
    "不要编造历史资料中没有体现的事实、经历或观点。"
    "如果缺乏足够依据，请明确表达不确定，而不是强行扮演。"
)

_STYLE_FOCUS_INSTRUCTION = (
    "【风格要求】模仿该成员的语言风格、话题倾向和表达习惯，"
    "但不夸大、不捏造、不超越已知信息的范围。"
)


def build_prompt(
    display_name: str,
    persona_summary: Optional[str],
    traits: Dict[str, Any],
    stats: Dict[str, Any],
    historical_messages: List[HistoricalMessage],
    current_message: str,
    conversation_context: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Assemble prompt_messages for a single simulation round.

    Returns:
        list of {"role": str, "content": str} dicts, ready for an LLM client.
    """
    system_content = _build_system_content(
        display_name=display_name,
        persona_summary=persona_summary,
        traits=traits,
        stats=stats,
        historical_messages=historical_messages,
    )
    user_content = _build_user_content(
        current_message=current_message,
        conversation_context=conversation_context,
    )
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content},
    ]


def build_prompt_from_input(sim_input: SimulationInput) -> List[Dict[str, str]]:
    """Convenience wrapper that accepts a SimulationInput dataclass."""
    return build_prompt(
        display_name=sim_input.display_name,
        persona_summary=sim_input.persona_summary,
        traits=sim_input.traits,
        stats=sim_input.stats,
        historical_messages=sim_input.historical_messages,
        current_message=sim_input.current_message,
        conversation_context=sim_input.conversation_context,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_system_content(
    display_name: str,
    persona_summary: Optional[str],
    traits: Dict[str, Any],
    stats: Dict[str, Any],
    historical_messages: List[HistoricalMessage],
) -> str:
    parts: List[str] = []

    # ── Mandatory boundaries (order matters) ──────────────────────────────
    parts.append(_SIMULATION_DISCLAIMER)
    parts.append(_NO_FABRICATION_CONSTRAINT)
    parts.append(_STYLE_FOCUS_INSTRUCTION)

    # ── Identity ──────────────────────────────────────────────────────────
    parts.append(f"\n【模拟对象】{display_name}")

    # ── Persona summary ───────────────────────────────────────────────────
    if persona_summary:
        parts.append(f"\n【人格概要】\n{persona_summary}")

    # ── Traits ────────────────────────────────────────────────────────────
    dominant = traits.get("dominant_topics", [])
    verbosity = traits.get("verbosity_level", "")
    activity = traits.get("activity_pattern", "")
    style_hints = traits.get("style_hints", [])

    trait_lines: List[str] = []
    if dominant:
        trait_lines.append(f"主要话题：{', '.join(dominant)}")
    if verbosity:
        trait_lines.append(f"发言风格：{verbosity}")
    if activity:
        trait_lines.append(f"活跃时段：{activity}")
    if style_hints:
        trait_lines.append(f"风格标签：{', '.join(style_hints)}")
    if trait_lines:
        parts.append("\n【特征画像】\n" + "\n".join(trait_lines))

    # ── Stats summary (selected fields only) ─────────────────────────────
    stats_block = _build_stats_summary(stats)
    if stats_block:
        parts.append("\n【统计摘要】\n" + stats_block)

    # ── Historical messages ───────────────────────────────────────────────
    if historical_messages:
        msg_lines = [
            f"[{m.sent_at.strftime('%Y-%m-%d %H:%M')}] "
            f"{m.normalized_content or m.content}"
            for m in historical_messages
        ]
        parts.append(
            "\n【历史发言样本（仅供参考风格，不代表当前观点）】\n"
            + "\n".join(msg_lines)
        )
    else:
        parts.append("\n【历史发言样本】暂无历史发言记录。")

    return "\n".join(parts)


def _build_stats_summary(stats: Dict[str, Any]) -> str:
    """
    Extract a concise, human-readable summary from the stats dict.

    Only renders fields that exist and are non-empty.  Never crashes on
    missing keys.  Does not dump raw JSON – each field gets a single line.

    Rendered fields (if present):
        message_count          – total messages in window
        top_keywords           – up to 5 top keywords with counts
        topic_distribution     – primary topic counts
        active_hours           – top-3 most active hours
    """
    lines: List[str] = []

    count = stats.get("message_count")
    if count is not None:
        lines.append(f"窗口内发言总数：{count}")

    kws = stats.get("top_keywords")
    if kws and isinstance(kws, list):
        top5 = kws[:5]
        kw_str = "、".join(
            f"{item['word']}({item['count']})" if isinstance(item, dict) else str(item)
            for item in top5
        )
        lines.append(f"高频词汇：{kw_str}")

    topic_dist = stats.get("topic_distribution")
    if topic_dist and isinstance(topic_dist, dict):
        sorted_topics = sorted(topic_dist.items(), key=lambda x: x[1], reverse=True)[:3]
        td_str = "、".join(f"{k}({v})" for k, v in sorted_topics)
        lines.append(f"主要话题分布：{td_str}")

    active_hours = stats.get("active_hours")
    if active_hours and isinstance(active_hours, dict):
        sorted_hours = sorted(active_hours.items(), key=lambda x: int(x[1]), reverse=True)[:3]
        ah_str = "、".join(f"{h}时({c}条)" for h, c in sorted_hours)
        lines.append(f"最活跃时段：{ah_str}")

    return "\n".join(lines)


def _build_user_content(
    current_message: str,
    conversation_context: Optional[str],
) -> str:
    parts: List[str] = []
    if conversation_context:
        parts.append(f"【对话背景】\n{conversation_context}\n")
    parts.append(f"【当前消息】\n{current_message}")
    return "\n".join(parts)
