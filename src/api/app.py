"""
NFA Forgotten Archive – read-only REST API.

All routes are mounted under /api/v1.

Endpoints
---------
GET /api/v1/health
GET /api/v1/groups
GET /api/v1/groups/{group_id}
GET /api/v1/groups/{group_id}/members
GET /api/v1/members/{member_id}
GET /api/v1/members/{member_id}/messages
GET /api/v1/members/{member_id}/profile/latest
GET /api/v1/members/{member_id}/profiles
"""

from fastapi import FastAPI

from src.api.routes import health, groups, members, legend

app = FastAPI(
    title="NFA Forgotten Archive API",
    description=(
        "Read-only API for querying groups, members, messages, "
        "and Persona Profile snapshots."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

_PREFIX = "/api/v1"

app.include_router(health.router, prefix=_PREFIX)
app.include_router(groups.router, prefix=_PREFIX)
app.include_router(members.router, prefix=_PREFIX)
app.include_router(legend.router, prefix=_PREFIX)
