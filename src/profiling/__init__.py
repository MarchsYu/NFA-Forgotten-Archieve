# Only pure, DB-free modules are imported at package level.
# This keeps `import src.profiling.profile_analyzers` and
# `import src.profiling.profile_builder` free of DB engine side-effects.
#
# Import the DB-dependent service directly when needed:
#   from src.profiling.profile_service import ProfileService, ProfilingResult
from src.profiling.profile_builder import ProfileBuilder, ProfileData, PROFILE_VERSION

__all__ = [
    "ProfileBuilder",
    "ProfileData",
    "PROFILE_VERSION",
]
