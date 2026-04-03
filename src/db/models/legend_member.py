import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import String, DateTime, Boolean, ForeignKey, CheckConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    from src.db.models.member import Member


class LegendMember(Base):
    __tablename__ = "legend_members"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    member_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("members.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    archive_status: Mapped[str] = mapped_column(String(32), nullable=False)
    simulation_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    restored_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    member: Mapped["Member"] = relationship("Member")

    __table_args__ = (
        CheckConstraint(
            "archive_status IN ('archived', 'restored')",
            name="ck_legend_member_archive_status",
        ),
        CheckConstraint(
            "NOT (archive_status = 'restored' AND simulation_enabled = true)",
            name="ck_legend_member_restored_no_simulation",
        ),
    )
