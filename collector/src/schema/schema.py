from pydantic import BaseModel
from typing import Dict, Any

class TopLevelSchema(BaseModel):
    event_type: str
    schema_version: str
    payload: dict