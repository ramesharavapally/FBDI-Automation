# api/routes.py - API route definitions
import os
from fastapi import APIRouter, HTTPException, Response , status
from fastapi.responses import StreamingResponse  
from io import StringIO
from fbdi.db import db
import logging

logger = logging.getLogger(__name__)


from fbdi.control_file.model.models import (    
    ObjectResponse,
    FBDIObjectListResponse,
)
from fbdi.control_file.services.control_file import (    
    get_control_file_data,    
)

router = APIRouter()



@router.get('/metadata/{object_name}' , response_model=ObjectResponse , status_code=status.HTTP_200_OK)
async def get_metadata(object_name: str):
    """
    Fetch metadata for a given object name, including control file fields and additional fields.
    Args:
        object_name (str): Name of the object.
    Returns:
        ObjectResponse: Metadata with control files and field details.
    Raises:
        HTTPException: If no control files are found or an error occurs.
    """
    try:
        _control_file_urls = await db.get_control_file_urls_by_object_name(object_name=object_name)        
        _control_file_list = []        
        for item in _control_file_urls:
            cf = list(item.keys())[0]
            url = item[cf]              
            _ , fields= await get_control_file_data(url)  

            _additioanl_fields = await db.get_additional_fields(object_name=object_name)
            
            for _field in _additioanl_fields:
                if not _field in fields:
                    fields = fields + [_field]  
            
            _control_file_list.append({cf:fields})
            
        if len(_control_file_list) > 0:
            return ObjectResponse(
                success=True,
                messgage=f"found {len(_control_file_list)} control files",
                files=_control_file_list,
                count=len(_control_file_list)
            )
        else:
            raise  HTTPException(status_code=status.HTTP_400_BAD_REQUEST , detail=f"No control files found")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST , detail=f"Error {e}")        
    



@router.get('/objects' , response_model=FBDIObjectListResponse, status_code=status.HTTP_200_OK)
async def get_fbdi_object_names():
    """
    Fetch all FBDI object names from the database.
    
    Returns:
        FBDIObjectListResponse: List of FBDI object names with success status and count.
    """
    try:
        object_names = await db.get_fbdi_object_names()        
        return FBDIObjectListResponse(                        
            object_names=object_names            
        )
    except Exception as e:
        logger.error(f"Error fetching FBDI object names: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")