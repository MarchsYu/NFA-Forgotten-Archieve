import uuid
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import String, DateTime, ForeignKey, Index, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.group import Group
    from src.db.models.message import Message
    from src.db.models.profile_snapshot import ProfileSnapshot


class Member(Base):
    __tablename__ = "members"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    external_member_id: Mapped[str] = mapped_column(String(128), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    nickname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Issue 7 Fix: status is the single source of truth for member state.
    # Valid values: "active", "inactive", "left", "banned", etc.
    # is_active is removed to avoid semantic overlap and potential inconsistency.
    # Use @property is_active (below) for convenience when needed.
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    joined_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    left_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Issue 5 Fix: server_default=func.now() as DB-side fallback
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
    group: Mapped["Group"] = relationship("Group", back_populates="members")
    messages: Mapped[List["Message"]] = relationship(
        "Message",
        back_populates="member",
        cascade="all, delete-orphan",
    )
    profile_snapshots: Mapped[List["ProfileSnapshot"]] = relationship(
        "ProfileSnapshot",
        back_populates="member",
        cascade="all, delete-orphan",
    )

    # Issue 7: Derived property for backward compatibility and convenience.
    # Returns True only when status == "active". This is computed, not stored.
    @property
    def is_active(self) -> bool:
        return self.status == "active"

    # Issue 7: Setter updates status to keep semantics consistent.
    @is_active.setter
    def is_active(self, value: bool) -> None:
        self.status = "active" if value else "inactive"

    __table_args__ = (
        UniqueConstraint("group_id", "external_member_id", name="uq_member_group_external"),
        Index("ix_members_group_id", "group_id"),
        # Issue 7 Fix: index on status instead of is_active for consistency
        Index("ix_members_group_id_status", "group_id", "status"),
    )
