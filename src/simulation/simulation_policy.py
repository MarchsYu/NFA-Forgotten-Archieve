"""
Simulation policy – eligibility guards for Persona Simulation.

All functions are pure (no DB access) so they can be tested without a session.

Three mandatory conditions must ALL be satisfied before simulation proceeds:
1. archive_status == "archived"   (member is in the Legend Archive)
2. simulation_enabled == True     (operator has explicitly unlocked simulation)
3. source_profile_snapshot_id is not None  (a profile anchor exists)

Failing any condition raises SimulationNotAllowedError with a clear message.
"""

from __future__ import annotations

import uuid
from typing import Optional


class SimulationNotAllowedError(ValueError):
    """Raised when a legend member does not meet simulation criteria."""


def assert_can_simulate(
    archive_status: str,
    simulation_enabled: bool,
    source_profile_snapshot_id: Optional[uuid.UUID],
) -> None:
    """
    Raise SimulationNotAllowedError if the legend member cannot be simulated.

    Args:
        archive_status:              Current LegendMember.archive_status.
        simulation_enabled:          Current LegendMember.simulation_enabled.
        source_profile_snapshot_id:  Current LegendMember.source_profile_snapshot_id.

    Raises:
        SimulationNotAllowedError: any condition is not satisfied.
    """
    if archive_status != "archived":
        raise SimulationNotAllowedError(
            f"Cannot simulate a legend member with archive_status='{archive_status}'. "
            f"Only 'archived' members may be simulated."
        )
    if not simulation_enabled:
        raise SimulationNotAllowedError(
            "Simulation is not enabled for this legend member. "
            "Enable it first via enable_simulation()."
        )
    if source_profile_snapshot_id is None:
        raise SimulationNotAllowedError(
            "Cannot simulate: no profile snapshot is anchored to this legend member "
            "(source_profile_snapshot_id is None). "
            "The member must have a Persona Profile snapshot before simulation can proceed."
        )
