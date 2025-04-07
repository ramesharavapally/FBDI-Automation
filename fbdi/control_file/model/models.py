# fbdi/model/models.py - Pydantic models for request and response validation
from typing import List, Optional , Dict
from pydantic import BaseModel, HttpUrl


class ObjectResponse(BaseModel):
    success: bool
    messgage : str
    files : List[Dict]
    count: int


class FBDIObjectListResponse(BaseModel):    
    object_names: List[str]