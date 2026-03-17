import uuid
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import BigInteger, String, Text, DateTime, ForeignKey, Index, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.group import Group
    from src.db.models.member import Member
    from src.db.models.message_topic import MessageTopic


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    member_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("members.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Issue 3: external_message_id is nullable to support messages without a platform ID.
    # Uniqueness is enforced via a partial unique index (group_id, external_message_id)
    # WHERE external_message_id IS NOT NULL — see __table_args__ below.
    # This allows safe re-runs of import jobs: duplicate (group_id, external_message_id)
    # pairs are rejected by the DB, while NULL rows are always inserted freely.
    external_message_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_type: Mapped[str] = mapped_column(String(32), nullable=False, default="text")
    reply_to_message_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("messages.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_file: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Issue 5 Fix: server_default=func.now() as DB-side fallback
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    group: Mapped["Group"] = relationship("Group", back_populates="messages")
    member: Mapped["Member"] = relationship("Member", back_populates="messages")
    reply_to_message: Mapped[Optional["Message"]] = relationship(
        "Message",
        remote_side="Message.id",
        foreign_keys="Message.reply_to_message_id",
    )
    # Issue 1 Fix: delete-orphan is kept here (Message is the owning side of MessageTopic).
    # passive_deletes=True tells SQLAlchemy to rely on DB-level CASCADE rather than
    # loading child rows into memory before deletion — avoids unnecessary SELECT on delete.
    topic_assignments: Mapped[List["MessageTopic"]] = relationship(
        "MessageTopic",
        back_populates="message",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        # Issue 6 Fix: postgresql_ops sets DESC order on sent_at for "latest first" queries.
        # This makes ORDER BY sent_at DESC index-only scans efficient on both indexes.
        Index(
            "ix_messages_group_id_sent_at",
            "group_id",
            "sent_at",
            postgresql_ops={"sent_at": "DESC"},
        ),
        Index(
            "ix_messages_member_id_sent_at",
            "member_id",
            "sent_at",
            postgresql_ops={"sent_at": "DESC"},
        ),
        Index("ix_messages_sent_at", "sent_at"),
        # Issue 3 Fix: partial unique index — enforces idempotent import per group,
        # but only when external_message_id is present (NULLs are excluded by WHERE clause).
        # SQLAlchemy renders this as:
        #   CREATE UNIQUE INDEX ... ON messages (group_id, external_message_id)
        #   WHERE external_message_id IS NOT NULL
        Index(
            "uix_messages_group_external_id",
            "group_id",
            "external_message_id",
            unique=True,
            postgresql_where="external_message_id IS NOT NULL",
        ),
    )
