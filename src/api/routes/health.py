from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    service: str


@router.get("/health", response_model=HealthResponse, summary="Health check")
def health() -> HealthResponse:
    """Return service liveness status."""
    return HealthResponse(status="ok", service="nfa-forgotten-archive")
