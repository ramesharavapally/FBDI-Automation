# services/control_file.py - Service layer for control file operations
import re
import requests
from typing import List, Tuple
from fastapi import HTTPException
from io import StringIO
import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)

async def __get_control_file_content(url: str) -> str:
    """
    Download a control file content from a URL without saving it locally using aiohttp
    
    Args:
        url: The URL of the control file
        
    Returns:
        str: The content of the control file
    """
    # logger.debug(f"Downloading control file from URL: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=response.status, detail=f"Error downloading the file: {response.reason}")
                # Assuming the control file is text-based
                return await response.text()
    
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error downloading the file: {str(e)}")

async def __extract_fields_from_content(content: str) -> List[str]:
    """
    Extract field names from control file content
    
    Args:
        content: The content of the control file
        
    Returns:
        list: List of field names
    """
    try:
        # Extract field names from the control file content
        return await _parse_control_file_content(content)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing control file: {str(e)}")

async def _parse_control_file_content(content: str) -> List[str]:
    """
    Parse control file content to extract field names
    
    Args:
        content: The content of the control file
        
    Returns:
        list: List of field names
    """
    # Find the position after "INTO TABLE"
    into_table_index = content.find('INTO TABLE')
    if into_table_index == -1:
        raise HTTPException(status_code=422, detail="Could not find 'INTO TABLE' in the control file")
    
    # Find the opening parenthesis of the field list
    open_paren_index = content.find('(', into_table_index)
    if open_paren_index == -1:
        raise HTTPException(status_code=422, detail="Could not find opening parenthesis after INTO TABLE")
    
    # Find the closing parenthesis (need to account for nested parentheses)
    depth = 1
    close_paren_index = -1
    for i in range(open_paren_index + 1, len(content)):
        if content[i] == '(':
            depth += 1
        elif content[i] == ')':
            depth -= 1
            if depth == 0:
                close_paren_index = i
                break
    
    if close_paren_index == -1:
        raise HTTPException(status_code=422, detail="Could not find matching closing parenthesis")
    
    # Extract the content between parentheses
    field_section = content[open_paren_index + 1:close_paren_index]
    
    # Split by lines and extract field names
    lines = field_section.split('\n')
    field_names = []
    
    for line in lines:
        # Skip the system derived values
        line = line.upper()
        if line.find('CONSTANT') != -1 or line.find('EXPRESSION') != -1 or line.find('FILLER') != -1 or str(line).strip() == 'END':
            continue
        # Check if line starts with a field name or with a comma followed by a field name
        match = re.match(r'^,?([A-Z0-9_]+)', line.strip())
        if match:
            field_names.append(match.group(1))
    if len(field_names) > 0:
        field_names = field_names+['END']
    
    return field_names

async def get_control_file_data(url: str) -> tuple:
    """
    Get control file content and extract fields without saving the file
    
    Args:
        url: The URL of the control file
    
    Returns:
        tuple: (content, fields)
    """
    content = await __get_control_file_content(url)
    fields = await __extract_fields_from_content(content)
    return content, fields