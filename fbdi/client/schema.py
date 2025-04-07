# client/schema.py - Module for handling client file uploads and schema extraction
import csv
import io
import json
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from fastapi import APIRouter, UploadFile, HTTPException, status, File, Form
from pydantic import BaseModel

# Setup logging
logger = logging.getLogger(__name__)

# Create API router
router = APIRouter()

# Pydantic models for request and response validation
class FileSeparator(BaseModel):
    """Model for file and separator mapping"""
    filename: str
    separator: Optional[str] = None

class SchemaResponse(BaseModel):
    """Response model for schema extraction operations"""
    success: bool
    message: str
    columns: List[str]
    count: int
    file_details: Optional[Dict[str, List[str]]] = None
    separators_used: Optional[Dict[str, str]] = None

async def extract_schema_from_csv(file_content: bytes, filename: str, separator: Optional[str] = None) -> List[str]:
    """
    Extract header/schema from a CSV file
    
    Args:
        file_content: Bytes content of the uploaded file
        filename: Original filename
        separator: Optional separator/delimiter to use. If None, tries to detect it.
        
    Returns:
        List[str]: List of column names
    """
    try:
        # Convert bytes to StringIO for pandas
        text_content = file_content.decode('utf-8')
        
        # Use separator if provided, otherwise let pandas detect it
        sep = separator if separator else None
        
        # Use pandas to read the CSV
        if sep:
            df = pd.read_csv(io.StringIO(text_content), sep=sep, nrows=0)
        else:
            # Try comma first (default)
            try:
                df = pd.read_csv(io.StringIO(text_content), nrows=0)
            except:
                # If comma fails, try to infer the separator
                df = pd.read_csv(io.StringIO(text_content), sep=None, engine='python', nrows=0)
        
        # Get the column names
        header = list(df.columns)
        
        # Clean up the header names (remove whitespace, etc.)
        header = [col.strip() for col in header if col.strip()]
        
        logger.debug(f"Extracted {len(header)} columns from {filename}")
        return header
    
    except Exception as e:
        logger.error(f"Error extracting schema from {filename}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unable to parse {filename} as CSV: {str(e)}"
        )

async def extract_schema_from_txt(file_content: bytes, filename: str, separator: Optional[str] = None) -> List[str]:
    """
    Extract header/schema from a TXT file
    
    Args:
        file_content: Bytes content of the uploaded file
        filename: Original filename
        separator: Optional separator/delimiter to use. If None, tries to detect it.
        
    Returns:
        List[str]: List of column names
    """
    try:
        # Convert bytes to string
        text_content = file_content.decode('utf-8')
        
        # Use pandas to read the text file
        sep = separator if separator else None
        
        if sep:
            # Use the provided separator
            df = pd.read_csv(io.StringIO(text_content), sep=sep, nrows=0)
        else:
            # Try common separators
            try:
                # Try tab first for text files
                df = pd.read_csv(io.StringIO(text_content), sep='\t', nrows=0)
            except:
                try:
                    # Then try comma
                    df = pd.read_csv(io.StringIO(text_content), sep=',', nrows=0)
                except:
                    # Finally try to infer the separator
                    df = pd.read_csv(io.StringIO(text_content), sep=None, engine='python', nrows=0)
        
        # Get the column names
        header = list(df.columns)
        
        # Clean up the header names
        header = [col.strip() for col in header if col.strip()]
        
        logger.debug(f"Extracted {len(header)} columns from {filename}")
        return header
    
    except Exception as e:
        logger.error(f"Error extracting schema from {filename}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unable to parse {filename}: {str(e)}"
        )

@router.post("/extract-schema/", response_model=SchemaResponse, status_code=status.HTTP_200_OK)
async def extract_schema(
    files: List[UploadFile] = File(...),
    default_separator: Optional[str] = Form(None),
    file_separators: Optional[str] = Form(None)    
):
    """
    Extract schema/header from uploaded files
    
    - Accepts multiple CSV or TXT files
    - Returns combined columns from all files
    - If multiple files are uploaded, columns are appended
    - Supports custom separator/delimiter for each file or auto-detection
    
    Args:
        files: List of files to process
        default_separator: Optional default separator to use for all files (e.g., ',', '|', '\t')
                   If not provided, ',' is used as default for CSV and auto-detection for TXT
        file_separators: Optional JSON string mapping filenames to separators
                   Format: [{"filename": "file1.csv", "separator": ","}, ...]
        description: Optional description of the files
        
    Returns:
        SchemaResponse: Response containing the extracted columns and file details
    """
    logger.debug(f"Received {len(files)} files for schema extraction")
    
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No files uploaded"
        )
    
    # Parse file_separators JSON if provided
    separator_mapping = {}
    if file_separators:        
        try:
            separator_list = json.loads(file_separators)
            for item in separator_list:
                if isinstance(item, dict) and "filename" in item and "separator" in item:
                    separator_mapping[item["filename"]] = item["separator"]
        except Exception as e:
            logger.warning(f"Could not parse file_separators JSON: {str(e)}")
            # Continue without the custom separators
    
    combined_columns = []
    file_details = {}
    separators_used = {}
    
    for file in files:
        try:
            # Read file content
            file_content = await file.read()
            
            # Determine which separator to use for this file
            # Priority: 1. File-specific separator 2. Default separator 3. Auto-detect
            file_separator = separator_mapping.get(file.filename, default_separator)
            
            # Store the separator used for this file in the response
            if file_separator:
                separators_used[file.filename] = file_separator
            else:
                separators_used[file.filename] = "auto-detected"
            
            # Extract schema based on file type (using the determined separator)
            if file.filename.lower().endswith('.csv'):
                columns = await extract_schema_from_csv(file_content, file.filename, file_separator)
            elif file.filename.lower().endswith('.txt'):
                columns = await extract_schema_from_txt(file_content, file.filename, file_separator)
            else:
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=f"Unsupported file type: {file.filename}. Only CSV and TXT files are supported."
                )
            
            # Store the columns for this file
            file_details[file.filename] = columns
            
            # Append new unique columns to combined list
            for col in columns:
                if col not in combined_columns:
                    combined_columns.append(col)
                    
            # Reset file position for potential reuse
            await file.seek(0)
            
        except HTTPException as e:
            # Re-raise HTTP exceptions
            raise e
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error processing file {file.filename}: {str(e)}"
            )
    
    # Create a summary for the response message
    if len(files) == 1:
        file = files[0]
        separator_used = separators_used.get(file.filename, "unknown")
        separator_info = f" using separator '{separator_used}'" if separator_used != "auto-detected" else " with auto-detected separator"
    else:
        separator_info = " with custom separators per file" if separator_mapping else ""
        if default_separator:
            separator_info = f" using default separator '{default_separator}'{separator_info}"
    
    return SchemaResponse(
        success=True,
        message=f"Successfully extracted schema from {len(files)} files{separator_info}",
        columns=combined_columns,
        count=len(combined_columns),
        file_details=file_details,
        separators_used=separators_used
    )