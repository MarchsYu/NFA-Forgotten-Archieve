"""
Topic taxonomy and keyword rules for rule-based classification (v1).

Each TopicDefinition carries:
  - topic_key   : stable identifier stored in the topics table
  - name        : human-readable label
  - description : short explanation for display / debugging
  - strong_kws  : keywords that yield confidence 0.85 when matched
  - weak_kws    : keywords that yield confidence 0.55 when matched
  - is_primary_eligible : whether this topic can be the primary label

Design notes
------------
- Keywords are matched against normalized_content (lowercased, whitespace-collapsed).
- A message can match multiple topics; the highest-confidence match is marked is_primary.
- Confidence is additive up to a cap: each additional keyword hit adds 0.05, max 0.95.
- All keys are lowercase ASCII with underscores – never change them once seeded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class TopicDefinition:
    topic_key: str
    name: str
    description: str
    strong_kws: List[str]   # confidence base 0.85
    weak_kws: List[str]     # confidence base 0.55
    is_primary_eligible: bool = True


# ---------------------------------------------------------------------------
# Taxonomy – 8 topics, MVP set
# ---------------------------------------------------------------------------
TOPICS: List[TopicDefinition] = [
    TopicDefinition(
        topic_key="casual_chat",
        name="Casual Chat",
        description="Everyday small talk, greetings, and social pleasantries.",
        strong_kws=[
            "哈哈", "哈哈哈", "lol", "haha", "😂", "😄", "早", "晚安",
            "好的", "ok", "okay", "嗯嗯", "哦哦", "好久不见", "最近怎么样",
        ],
        weak_kws=[
            "你好", "hi", "hello", "hey", "在吗", "在不在", "怎么了",
            "没事", "随便", "都行", "无所谓",
        ],
    ),
    TopicDefinition(
        topic_key="technical",
        name="Technical",
        description="Programming, engineering, tools, bugs, and technical problem-solving.",
        strong_kws=[
            "bug", "error", "exception", "代码", "编程", "python", "java",
            "javascript", "typescript", "sql", "api", "git", "docker",
            "kubernetes", "deploy", "部署", "服务器", "数据库", "算法",
            "函数", "class", "import", "null", "undefined", "stack trace",
        ],
        weak_kws=[
            "问题", "报错", "崩了", "跑不起来", "怎么写", "怎么用",
            "文档", "库", "框架", "版本", "更新", "安装",
        ],
    ),
    TopicDefinition(
        topic_key="gaming",
        name="Gaming",
        description="Video games, mobile games, game strategy, and gaming culture.",
        strong_kws=[
            "游戏", "打游戏", "王者", "英雄联盟", "lol", "原神", "steam",
            "minecraft", "fps", "moba", "rpg", "boss", "副本", "刷图",
            "开黑", "上分", "rank", "段位", "皮肤", "装备",
        ],
        weak_kws=[
            "玩", "赢了", "输了", "队友", "对手", "操作", "技能",
        ],
    ),
    TopicDefinition(
        topic_key="emotion",
        name="Emotion",
        description="Expressions of feelings, mood, stress, happiness, or sadness.",
        strong_kws=[
            "难过", "伤心", "哭", "😭", "😢", "崩溃", "焦虑", "压力",
            "开心", "高兴", "幸福", "感动", "委屈", "生气", "愤怒",
            "失望", "绝望", "孤独", "寂寞", "心情", "情绪",
        ],
        weak_kws=[
            "好累", "好烦", "烦死了", "太难了", "受不了", "好棒",
            "太好了", "不开心", "有点", "感觉",
        ],
    ),
    TopicDefinition(
        topic_key="argument",
        name="Argument / Conflict",
        description="Disagreements, debates, conflicts, or confrontational exchanges.",
        strong_kws=[
            "你错了", "不对", "胡说", "瞎说", "你说的不对", "我不同意",
            "争", "吵", "骂", "滚", "闭嘴", "你懂什么", "别废话",
            "凭什么", "不服", "有问题", "你有毛病",
        ],
        weak_kws=[
            "但是", "然而", "不过", "其实", "明明", "怎么可能",
            "不是这样的", "你搞错了",
        ],
    ),
    TopicDefinition(
        topic_key="planning",
        name="Planning / Coordination",
        description="Scheduling, making plans, coordinating activities or meetings.",
        strong_kws=[
            "计划", "安排", "时间", "几点", "什么时候", "周末", "明天",
            "后天", "下周", "约", "见面", "会议", "开会", "讨论",
            "确认", "提醒", "deadline", "ddl", "截止",
        ],
        weak_kws=[
            "今天", "明天", "下午", "上午", "晚上", "有空", "方便",
            "一起", "我们", "大家",
        ],
    ),
    TopicDefinition(
        topic_key="meme",
        name="Meme / Humor",
        description="Internet memes, jokes, funny content, and humorous references.",
        strong_kws=[
            "哈哈哈哈", "笑死", "笑哭", "绷不住", "蚌埠住了", "yyds",
            "awsl", "xswl", "草", "离谱", "抽象", "整活", "梗",
            "表情包", "meme", "笑话", "段子",
        ],
        weak_kws=[
            "哈哈", "lol", "funny", "好笑", "搞笑", "逗", "玩笑",
        ],
    ),
    TopicDefinition(
        topic_key="question",
        name="Question / Help Request",
        description="Asking for information, help, opinions, or clarification.",
        strong_kws=[
            "怎么", "如何", "为什么", "是什么", "哪里", "哪个", "谁",
            "能不能", "可以吗", "帮我", "请问", "有没有", "知道吗",
            "what", "how", "why", "where", "who", "when", "?", "？",
        ],
        weak_kws=[
            "想知道", "不懂", "不明白", "求", "麻烦", "帮忙",
        ],
    ),
]

# Lookup by topic_key for O(1) access
TOPIC_MAP: dict[str, TopicDefinition] = {t.topic_key: t for t in TOPICS}

# Confidence parameters
STRONG_BASE = 0.85
WEAK_BASE = 0.55
EXTRA_KW_BONUS = 0.05   # per additional keyword hit beyond the first
MAX_CONFIDENCE = 0.95
