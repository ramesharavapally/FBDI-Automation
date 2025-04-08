# fbdi/transform/api/routes.py - API route definitions for transformation endpoints
import os
from fastapi import APIRouter, UploadFile, HTTPException, status, File, Form
from fastapi.responses import Response
import logging

logger = logging.getLogger(__name__)

from fbdi.generator.services.transform_service import (
    process_mapping_and_generate_csvs,
    get_transformed_filename
)

router = APIRouter()

@router.post("/transform/")
async def transform_data_with_mapping(
    source_file: UploadFile = File(...),
<<<<<<< HEAD
    mapping_json: str = Form(...),
):
    """
    Transform source data using a mapping JSON string and return zipped CSV files
    
    - Upload a source data file (CSV or Excel)
    - Provide a mapping JSON string
    - Process the mapping to generate CSV files
=======
    mapping_file: UploadFile = File(...),
):
    """
    Transform source data using a mapping file and return zipped CSV files
    
    - Upload a source data file (CSV or Excel)
    - Upload a mapping file (Excel with mapping sheets)
    - Process each mapping sheet to generate CSV files
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd
    - Return a zip file containing all generated CSVs
    
    Args:
        source_file: Source data file (CSV or Excel)
<<<<<<< HEAD
        mapping_json: Mapping data as a JSON string
        
    Returns:
        Response: Zip file containing generated CSV files
    
    Example JSON mapping structure:
    {
      "Sheet1": [
        {
          "Source Column": "source1",
          "Control Column": "target1",
          "Default Value": {"type":"sql",
                            "value":"select 1 from dual"}
        },    
        {
          "Source Column": "source2",
          "Control Column": "target2",
          "Default Value": {"type":"sequence",
                            "value":"YYYYMMDDHHMISS"}
        },    
        {
          "Source Column": "source2",
          "Control Column": "target2",
          "Default Value": {"type":"constant",
                            "value":"123"}
        },    
      ],
      "Sheet2": [    
         ...
      ]
    }
    """
    try:
        # Read file contents
        source_content = await source_file.read()
        
        if not source_content or not mapping_json:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Empty source file or mapping JSON provided"
=======
        mapping_file: Mapping file (Excel with mapping sheets)
        
    Returns:
        Response: Zip file containing generated CSV files
    """
    try:
        # Check file types
        if not mapping_file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Mapping file must be an Excel file (.xlsx or .xls)"
            )
        
        # Read file contents
        source_content = await source_file.read()
        mapping_content = await mapping_file.read()
        
        if not source_content or not mapping_content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Empty file(s) uploaded"
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd
            )
        
        # Process files and generate zip
        zip_content = await process_mapping_and_generate_csvs(
            source_content,
<<<<<<< HEAD
            mapping_json,
            source_file.filename            
        )
        
        # Determine the output filename
        output_filename = await get_transformed_filename(source_file.filename)
=======
            mapping_content,
            source_file.filename,
            mapping_file.filename
        )
        
        # Determine the output filename
        output_filename = get_transformed_filename(source_file.filename)
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd
        
        # Return the zip file as a response
        return Response(
            content=zip_content,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={output_filename}"
            }
        )
    
    except HTTPException as e:
        # Re-raise HTTP exceptions
        raise e
    except Exception as e:
        logger.error(f"Error in transformation endpoint: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process files: {str(e)}"
        )
