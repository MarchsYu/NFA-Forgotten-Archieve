from __future__ import annotations

from typing import Generic, List, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PagedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response envelope.

    Fields
    ------
    items   : the current page of results
    total   : total number of matching rows (may be None if not computed)
    limit   : page size used for this request
    offset  : offset used for this request
    """
    items: List[T]
    total: int
    limit: int
    offset: int
