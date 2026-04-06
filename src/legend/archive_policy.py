"""
Archive policy – eligibility rules and state-transition guards.

All functions are pure (no DB access) so they can be tested without a session.
"""

from __future__ import annotations

from typing import Optional

# Valid archive_status values
STATUS_ARCHIVED = "archived"
STATUS_RESTORED = "restored"

# Member statuses that qualify for archiving without force
_ELIGIBLE_STATUSES = {"left"}


class ArchiveNotEligibleError(ValueError):
    """Raised when a member does not meet archiving criteria."""


class InvalidStatusTransitionError(ValueError):
    """Raised when a requested state transition is not allowed."""


def assert_eligible_for_archive(
    member_status: str,
    force: bool = False,
) -> None:
    """
    Raise ``ArchiveNotEligibleError`` if the member cannot be archived.

    Default eligibility: ``member.status == "left"``.
    Pass ``force=True`` to bypass the status check (manual override).

    Args:
        member_status: Current value of ``Member.status``.
        force:         Skip eligibility check when True.

    Raises:
        ArchiveNotEligibleError: member is not eligible and force is False.
    """
    if force:
        return
    if member_status not in _ELIGIBLE_STATUSES:
        raise ArchiveNotEligibleError(
            f"Member with status '{member_status}' is not eligible for archiving. "
            f"Eligible statuses: {sorted(_ELIGIBLE_STATUSES)}. "
            f"Use force=True to override."
        )


def assert_can_restore(archive_status: str) -> None:
    """
    Raise ``InvalidStatusTransitionError`` if the legend member cannot be restored.

    Only ``archived`` members can be restored.

    Args:
        archive_status: Current ``LegendMember.archive_status``.

    Raises:
        InvalidStatusTransitionError: member is not in ``archived`` state.
    """
    if archive_status != STATUS_ARCHIVED:
        raise InvalidStatusTransitionError(
            f"Cannot restore a legend member with status '{archive_status}'. "
            f"Only '{STATUS_ARCHIVED}' members can be restored."
        )


def assert_can_toggle_simulation(archive_status: str) -> None:
    """
    Raise ``InvalidStatusTransitionError`` if simulation cannot be toggled.

    Simulation may only be enabled/disabled on ``archived`` members.
    A ``restored`` member's simulation flag is always False and cannot be
    re-enabled until the member is archived again.

    Args:
        archive_status: Current ``LegendMember.archive_status``.

    Raises:
        InvalidStatusTransitionError: member is not in ``archived`` state.
    """
    if archive_status != STATUS_ARCHIVED:
        raise InvalidStatusTransitionError(
            f"Cannot toggle simulation for a legend member with status "
            f"'{archive_status}'. Simulation is only available for "
            f"'{STATUS_ARCHIVED}' members."
        )
