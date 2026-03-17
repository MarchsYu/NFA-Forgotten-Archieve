# Task 2: 实现第一阶段数据库层

## 任务目标

实现 NFA Forgotten Archive 第一阶段的数据库基础结构，支持群聊历史归档、成员消息存储、话题分类和 Persona Profile 生成。

---

## 核心表结构

### 1. groups — 群组信息
| 字段 | 类型 | 说明 |
|------|------|------|
| id | UUID | 主键 |
| platform | String(32) | 平台标识 (qq/wechat/discord) |
| external_group_id | String(128) | 外部群组ID |
| name | String(255) | 群组名称 |
| metadata_json | JSONB | 扩展元数据 |
| created_at | DateTime | 创建时间 |
| updated_at | DateTime | 更新时间 |

**约束**: 唯一索引 (platform, external_group_id)

---

### 2. members — 群成员信息
| 字段 | 类型 | 说明 |
|------|------|------|
| id | UUID | 主键 |
| group_id | UUID | 外键 → groups.id |
| external_member_id | String(128) | 外部成员ID |
| display_name | String(255) | 显示名称 |
| nickname | String(255) | 昵称（可空） |
| status | String(32) | active/left/archived |
| is_active | Boolean | 是否活跃 |
| joined_at | DateTime | 加入时间 |
| left_at | DateTime | 离开时间 |
| created_at | DateTime | 创建时间 |
| updated_at | DateTime | 更新时间 |

**约束**: 唯一索引 (group_id, external_member_id)

---

### 3. messages — 聊天记录
| 字段 | 类型 | 说明 |
|------|------|------|
| id | BigInteger | 主键，自增 |
| group_id | UUID | 外键 → groups.id |
| member_id | UUID | 外键 → members.id |
| external_message_id | String(128) | 外部消息ID |
| sent_at | DateTime | 发送时间 |
| content | Text | 原始内容 |
| normalized_content | Text | 标准化内容 |
| content_type | String(32) | 内容类型 (默认 text) |
| reply_to_message_id | BigInteger | 回复消息ID（自关联） |
| source_file | String(512) | 来源文件 |
| raw_payload | JSONB | 原始数据 |
| created_at | DateTime | 创建时间 |

**索引**: (group_id, sent_at), (member_id, sent_at), (sent_at)

---

### 4. topics — 话题分类
| 字段 | 类型 | 说明 |
|------|------|------|
| id | SmallInteger | 主键，自增 |
| topic_key | String(64) | 唯一标识 (casual_chat/technical/gaming) |
| name | String(128) | 话题名称 |
| description | Text | 描述 |
| parent_topic_id | SmallInteger | 父话题ID（自关联） |
| is_active | Boolean | 是否启用 |
| created_at | DateTime | 创建时间 |
| updated_at | DateTime | 更新时间 |

**约束**: 唯一索引 topic_key

---

### 5. message_topics — 消息话题关联
| 字段 | 类型 | 说明 |
|------|------|------|
| message_id | BigInteger | 外键 → messages.id |
| topic_id | SmallInteger | 外键 → topics.id |
| classifier_version | String(64) | 分类器版本 |
| confidence | Numeric(5,4) | 置信度 |
| is_primary | Boolean | 是否主要话题 |
| evidence | JSONB | 规则命中证据 |
| assigned_at | DateTime | 分配时间 |

**主键**: (message_id, topic_id, classifier_version) — 支持多版本并存

---

### 6. profile_snapshots — 人格画像快照
| 字段 | 类型 | 说明 |
|------|------|------|
| id | UUID | 主键 |
| group_id | UUID | 外键 → groups.id |
| member_id | UUID | 外键 → members.id |
| profile_version | String(64) | 画像版本 |
| snapshot_at | DateTime | 快照时间 |
| window_start | DateTime | 统计窗口开始 |
| window_end | DateTime | 统计窗口结束 |
| source_message_count | Integer | 来源消息数 |
| persona_summary | Text | 人格摘要 |
| traits | JSONB | 特征数据 |
| stats | JSONB | 统计数据 |
| created_at | DateTime | 创建时间 |

**约束**: 唯一索引 (member_id, profile_version, window_start, window_end)

---

## 关系图

```
groups (1) ────< (N) members
   │                  │
   │ 1:N              │ 1:N
   ▼                  ▼
messages (1) ──< (N) profile_snapshots
   │
   │ 1:N
   ▼
message_topics (N) >─── (1) topics
```

---

## 文件清单

```
src/
├── config.py
└── db/
    ├── base.py
    ├── session.py
    ├── __init__.py
    └── models/
        ├── __init__.py
        ├── group.py
        ├── member.py
        ├── message.py
        ├── topic.py
        ├── message_topic.py
        └── profile_snapshot.py

scripts/
└── init_db.py
```

---

## 使用方法

```bash
# 设置数据库连接
export DATABASE_URL="postgresql://user:password@host:port/dbname"

# 初始化数据库
python scripts/init_db.py
```

---

## 技术要点

- SQLAlchemy 2.0 风格（Mapped, mapped_column）
- 完整类型注解支持
- TYPE_CHECKING 避免循环导入
- 所有外键设置 ondelete 策略
- 复合主键支持 MessageTopic 多版本分类
- 预留 status 字段为 Legend Archive 扩展

---

## 后续建议

1. 引入 Alembic 管理数据库迁移
2. 添加 Topic 种子数据初始化脚本
3. 根据查询模式优化索引
4. 调整连接池参数适配生产环境
