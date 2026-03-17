from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Identity,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.message_topic import MessageTopic


class Topic(Base):
    __tablename__ = "topics"

    # Issue 2 Fix: Use Integer + Identity() for reliable PostgreSQL autoincrement
    # Identity() is the SQLAlchemy 2.0+ recommended way for PostgreSQL SERIAL/GENERATED ALWAYS
    id: Mapped[int] = mapped_column(
        Integer,
        Identity(start=1, increment=1, always=True),
        primary_key=True,
    )
    topic_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # parent_topic_id uses Integer to match the updated id column type
    parent_topic_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("topics.id", ondelete="SET NULL"),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Issue 5 Fix: server_default=func.now() as DB-side fallback to reduce clock drift
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    parent_topic: Mapped[Optional["Topic"]] = relationship(
        "Topic",
        remote_side="Topic.id",
        foreign_keys="Topic.parent_topic_id",
    )
    # Issue 1 Fix: remove delete-orphan from Topic side; orphan ownership lives on Message side.
    # passive_deletes=True lets the DB CASCADE handle row removal when a Topic is deleted.
    message_assignments: Mapped[List["MessageTopic"]] = relationship(
        "MessageTopic",
        back_populates="topic",
        cascade="all",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_topics_parent_topic_id", "parent_topic_id"),
    )
