# fbdi/transform/services/transform_service.py - Service for transforming source data using mappings
import io
import os
import zipfile
import pandas as pd
import logging
from typing import Dict, List, Any, Optional, BinaryIO
from tempfile import NamedTemporaryFile
from datetime import datetime
from fbdi.db import db
import json

logger = logging.getLogger(__name__)



# Example JSON mapping structure
# {
#   "Sheet1": [
#     {
#       "Source Column": "source1",
#       "Control Column": "target1",
#       "Default Value": {"type":"sql",
#                         "value":"select 1 from dual"}
#     },    
#     {
#       "Source Column": "source2",
#       "Control Column": "target2",
#       "Default Value": {"type":"sequence",
#                         "value":"YYYYMMDDHHMISS"}
#     },    
#     {
#       "Source Column": "source2",
#       "Control Column": "target2",
#       "Default Value": {"type":"constant",
#                         "value":"123"}
#     },    
#   ],
#   "Sheet2": [    
#      ...
#   ]
# }


async def process_mapping_and_generate_csvs(
    source_file_content: bytes,
    mapping_file_content: str,
    source_filename: str,    
) -> bytes:
    """
    Process source file using mapping file to generate CSV files
    
    Args:
        source_file_content: Content of the source file
        mapping_file_content: Content of the JSON mapping (as bytes)
        source_filename: Name of the source file        
        
    Returns:
        bytes: Zip file containing generated CSV files
    """
    try:
        # Create byte stream for source file processing
        source_stream = io.BytesIO(source_file_content)
        
        # Detect file type for source file and read accordingly
        if source_filename.lower().endswith('.csv') or source_filename.lower().endswith('.txt'):
            source_df = pd.read_csv(source_stream, sep='|')        
        else:            
            raise Exception('Invalid source file format, valid formats are csv/txt')
        
        # Parse JSON mapping content        
        mapping_data = json.loads(mapping_file_content)
        
        # Each key in the mapping_data represents a sheet (similar to Excel sheets)
        sheet_names = list(mapping_data.keys())
        
        logger.debug(f"Found {len(sheet_names)} mapping sheets in JSON mapping")
        
        # Create an in-memory zip file
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Process each mapping sheet
            for sheet_name in sheet_names:
                try:
                    # Convert JSON mapping to DataFrame for consistent processing
                    mapping_list = mapping_data[sheet_name]
                    mapping_df = pd.DataFrame(mapping_list)
                    
                    # Check if mapping sheet has required columns
                    if 'Source Column' not in mapping_df.columns or 'Control Column' not in mapping_df.columns:
                        logger.warning(f"Skipping sheet {sheet_name}: Missing required columns 'Source Column' or 'Control Column'")
                        continue
                    
                    # Create new dataframe for the output
                    output_df = pd.DataFrame()
                    
                    # Apply the mappings
                    for _, row in mapping_df.iterrows():
                        source_col = row['Source Column']
                        target_col = row['Control Column']
                        default_value = row.get('Default Value', None)

                        if pd.isna(source_col) or source_col not in source_df.columns:
                            # Handle default values with new structure
                            if pd.notna(default_value):
                                if isinstance(default_value, dict):
                                    # Process structured default value
                                    value_type = default_value.get('type', '').lower()
                                    value_content = default_value.get('value', '')
                                    
                                    if value_type == 'sql':
                                        # Execute SQL query to get value
                                        sql_result = await db.execute_sql_query(value_content)
                                        output_df[target_col] = sql_result.iloc[0, 0] if not sql_result.empty else None
                                    elif value_type == 'sequence':
                                        # Generate sequence based on format
                                        format_pattern = value_content if value_content else 'YYYYMMDDHHMISS'
                                        timestamp = await generate_sequence(format_pattern)
                                        output_df[target_col] = timestamp
                                    elif value_type == 'constant':
                                        # Use constant value
                                        output_df[target_col] = value_content
                                    else:
                                        # Unknown type, use as-is
                                        logger.warning(f"Unknown default value type: {value_type}, using raw value")
                                        output_df[target_col] = default_value
                                elif str(default_value).upper() == 'SEQUENCE':
                                    # Legacy sequence support
                                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                                    output_df[target_col] = timestamp
                                else:
                                    # Use the specified default value as-is
                                    output_df[target_col] = default_value
                            else:
                                output_df[target_col] = None                                                    
                        else:
                            # Map the column
                            output_df[target_col] = source_df[source_col]
                    
                    # Save to CSV in memory
                    output_buffer = io.StringIO()
                    output_df.to_csv(output_buffer, index=False)
                    
                    # Add to zip file
                    csv_filename = f"{sheet_name.replace(' ', '_')}.csv"
                    zip_file.writestr(csv_filename, output_buffer.getvalue())
                    
                    logger.debug(f"Generated CSV for sheet {sheet_name} with {len(output_df)} rows")
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                    # Continue with other sheets
        
        # Get the zip file as bytes
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
        
    except json.JSONDecodeError as je:
        logger.error(f"Error parsing JSON mapping: {str(je)}")
        raise Exception(f"Invalid JSON mapping format: {str(je)}")
    except Exception as e:
        logger.error(f"Error in transformation process: {str(e)}")
        raise Exception(f"Failed to process files: {str(e)}")



async def generate_sequence(format_pattern: str) -> str:
    """
    Generate a sequence based on the given format pattern
    
    Args:
        format_pattern: Format pattern for the sequence
            - YYYY: 4-digit year
            - MM: 2-digit month
            - DD: 2-digit day
            - HH: 2-digit hour (24-hour)
            - MI: 2-digit minute
            - SS: 2-digit second
            
    Returns:
        str: Generated sequence
    """
    now = datetime.now()
    
    # Create mapping for format tokens
    format_map = {
        'YYYY': now.strftime('%Y'),
        'MM': now.strftime('%m'),
        'DD': now.strftime('%d'),
        'HH': now.strftime('%H'),
        'MI': now.strftime('%M'),
        'SS': now.strftime('%S')
    }
    
    # Replace each token in the pattern
    result = format_pattern
    for token, value in format_map.items():
        result = result.replace(token, value)
    
    return result
    


async def get_transformed_filename(source_filename: str) -> str:
    """
    Generate a name for the transformed zip file
    
    Args:
        source_filename: Original source filename
        
    Returns:
        str: Name for the transformed zip file
    """
    base_name = os.path.splitext(os.path.basename(source_filename))[0]
    return f"{base_name}_transformed.zip"
