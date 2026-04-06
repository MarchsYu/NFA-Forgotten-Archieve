"""
NFA Forgotten Archive – REST API.

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

GET  /api/v1/legend/members
GET  /api/v1/legend/members/{member_id}
POST /api/v1/legend/members/{member_id}/archive
POST /api/v1/legend/members/{member_id}/restore
POST /api/v1/legend/members/{member_id}/enable-simulation
POST /api/v1/legend/members/{member_id}/disable-simulation
"""

from fastapi import FastAPI

from src.api.routes import health, groups, members, legend

app = FastAPI(
    title="NFA Forgotten Archive API",
    description=(
        "API for querying groups, members, messages, "
        "Persona Profile snapshots, and the Legend Archive."
    ),
    version="0.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

_PREFIX = "/api/v1"

app.include_router(health.router, prefix=_PREFIX)
app.include_router(groups.router, prefix=_PREFIX)
app.include_router(members.router, prefix=_PREFIX)
app.include_router(legend.router, prefix=_PREFIX)
