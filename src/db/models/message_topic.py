from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Integer, String, Boolean, DateTime, ForeignKey, Numeric, Index, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.message import Message
    from src.db.models.topic import Topic


class MessageTopic(Base):
    __tablename__ = "message_topics"

    message_id: Mapped[int] = mapped_column(
        BigInteger,
        # ondelete="CASCADE" + passive_deletes=True on Message side handles DB-level cleanup
        ForeignKey("messages.id", ondelete="CASCADE"),
        primary_key=True,
    )
    # Issue 2 Fix: topic_id uses Integer to match Topic.id (was SmallInteger)
    topic_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    )
    classifier_version: Mapped[str] = mapped_column(String(64), nullable=False, primary_key=True)
    # Issue 4 Fix: Numeric(5,4) stores up to 4 decimal places of precision.
    # Python type is Decimal (not float) to avoid IEEE-754 rounding surprises during
    # serialization and comparison. Use Decimal("0.9500") rather than 0.95 at call sites.
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    evidence: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Issue 5 Fix: server_default=func.now() as DB-side fallback
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="topic_assignments")
    topic: Mapped["Topic"] = relationship("Topic", back_populates="message_assignments")

    __table_args__ = (
        Index("ix_message_topics_topic_id", "topic_id"),
        Index("ix_message_topics_message_id", "message_id"),
        Index("ix_message_topics_classifier_version", "classifier_version"),
    )
