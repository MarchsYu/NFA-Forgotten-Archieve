from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, SmallInteger, String, Boolean, DateTime, ForeignKey, Numeric, Index
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
        ForeignKey("messages.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    )
    classifier_version: Mapped[str] = mapped_column(String(64), nullable=False, primary_key=True)
    confidence: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    evidence: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="topic_assignments")
    topic: Mapped["Topic"] = relationship("Topic", back_populates="message_assignments")

    __table_args__ = (
        Index("ix_message_topics_topic_id", "topic_id"),
        Index("ix_message_topics_message_id", "message_id"),
        Index("ix_message_topics_classifier_version", "classifier_version"),
    )
