from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import SmallInteger, String, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.message_topic import MessageTopic


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    topic_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parent_topic_id: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        ForeignKey("topics.id", ondelete="SET NULL"),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    parent_topic: Mapped[Optional["Topic"]] = relationship(
        "Topic",
        remote_side="Topic.id",
        foreign_keys="Topic.parent_topic_id",
    )
    message_assignments: Mapped[List["MessageTopic"]] = relationship(
        "MessageTopic",
        back_populates="topic",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_topics_parent_topic_id", "parent_topic_id"),
    )
