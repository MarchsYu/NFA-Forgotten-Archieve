import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.group import Group
    from src.db.models.member import Member
    from src.db.models.profile_snapshot import ProfileSnapshot


class LegendMember(Base):
    """
    Archived record for a member who has left the group.

    One row per member (UNIQUE on member_id).  The row is created on first
    archive and reused on subsequent archive/restore cycles – it is never
    deleted.

    archive_status values
    ---------------------
    "archived"  – member is currently archived (default after archive())
    "restored"  – member was restored; simulation_enabled is forced False

    simulation_enabled
    ------------------
    Controls whether Persona Simulation may use this legend member as input.
    Defaults to False.  Must be explicitly enabled via enable_simulation().
    Automatically set to False on restore().
    """

    __tablename__ = "legend_members"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    member_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("members.id", ondelete="RESTRICT"),
        nullable=False,
    )
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("groups.id", ondelete="RESTRICT"),
        nullable=False,
    )

    # ── Archive state ────────────────────────────────────────────────────────
    archive_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="archived",
    )
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    archived_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    archived_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # ── Profile anchor ───────────────────────────────────────────────────────
    source_profile_snapshot_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("profile_snapshots.id", ondelete="SET NULL"),
        nullable=True,
    )

    # ── Member identity snapshot (denormalised for resilience) ───────────────
    member_display_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    member_external_id_snapshot: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    member_status_snapshot: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # ── Simulation gate ──────────────────────────────────────────────────────
    simulation_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # ── Audit timestamps ─────────────────────────────────────────────────────
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

    # ── Relationships ────────────────────────────────────────────────────────
    member: Mapped["Member"] = relationship("Member")
    group: Mapped["Group"] = relationship("Group")
    source_profile_snapshot: Mapped[Optional["ProfileSnapshot"]] = relationship("ProfileSnapshot")

    __table_args__ = (
        UniqueConstraint("member_id", name="uq_legend_members_member_id"),
        CheckConstraint(
            "archive_status IN ('archived', 'restored')",
            name="ck_legend_members_archive_status_valid",
        ),
        CheckConstraint(
            "NOT (archive_status = 'restored' AND simulation_enabled = true)",
            name="ck_legend_members_restored_simulation_disabled",
        ),
        # Primary query pattern: list by group, filter by status, order by archived_at
        Index(
            "ix_legend_members_group_status_archived",
            "group_id", "archive_status", "archived_at",
        ),
        # Simulation gate query: find all simulation-enabled archived members
        Index(
            "ix_legend_members_simulation_status",
            "simulation_enabled", "archive_status",
        ),
        # Profile anchor lookup
        Index(
            "ix_legend_members_profile_snapshot_id",
            "source_profile_snapshot_id",
        ),
    )
