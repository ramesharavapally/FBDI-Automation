# db.py - Module for accessing and managing database operations via API
import logging
from typing import Dict, List, Any, Optional
import aiohttp
from pydantic import BaseModel, HttpUrl
import asyncio
import oracledb
from fbdi.utils.config import config , get_db_confg
import json
import pandas as pd
logger = logging.getLogger(__name__)



class ObjectModel(BaseModel):
    id: int
    name: str
    controlfiles: List[str]
    additionalColumns : List[str]

class ConfigModel(BaseModel):    
    control_file_prefix: str
    version: str
    control_file_sufix: str
    

async def __getConnection() -> oracledb.Connection:
    """
    Asynchronously establishes and returns a connection to an Oracle database.
    This function retrieves the database configuration, constructs a Data Source Name (DSN),
    and uses it to create an asynchronous connection to the database.
    Returns:
        oracledb.Connection: An asynchronous connection object to the Oracle database.
    Raises:
        oracledb.DatabaseError: If there is an error while connecting to the database.
    """
    
    db_config = await get_db_confg()

    dsn = oracledb.makedsn(
        host=db_config.get("host"),
        port=db_config.get("port"),
        service_name=db_config.get("service_name")
    )
    return await oracledb.connect_async(
        user=db_config.get("username"),
        password=db_config.get("password"),
        dsn=dsn,
    )    



async def __get_fbdi_config() -> ConfigModel:
    """
    Asynchronously retrieves the FBDI configuration from the database.
    This function connects to the database, executes a query to fetch the 
    FBDI configuration details, and returns the data as a ConfigModel instance. 
    If no configuration is found, it logs an error and raises a ValueError.
    Returns:
        ConfigModel: An instance of ConfigModel containing the FBDI configuration 
        details (control file prefix, version, and control file suffix).
    Raises:
        ValueError: If no configuration is found in the database.
    """
    
    conn = await __getConnection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT CONTROL_FILE_PREFIX , VERSION , CONTROL_FILE_SUFFIX FROM FBDI_CONFIG")
            row = await cursor.fetchone()
            if row:
                data = {
                    "control_file_prefix": row[0],
                    "version": row[1],
                    "control_file_sufix": row[2]
                }
            else:
                logger.error("No configuration found in the database")
                raise ValueError("No configuration found in the database")
    finally:
        await conn.close()
    return ConfigModel(**data)

async def __get_object_by_name(name: str) -> Optional[ObjectModel]:
    """
    Asynchronously retrieves an object from the database by its name.
    This function connects to the database, executes a query to fetch an object
    from the `FBDI_OBJECT` table based on the provided name, and returns the
    result as an instance of `ObjectModel`. If the object is not found or an
    error occurs, it returns `None`.
    Args:
        name (str): The name of the object to retrieve.
    Returns:
        Optional[ObjectModel]: An instance of `ObjectModel` if the object is found,
        otherwise `None`.
    Raises:
        Exception: Logs a warning if an error occurs during the database operation.
    """
    
    try:            
        conn = await __getConnection()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT * FROM FBDI_OBJECT WHERE NAME = :name", name=name)
                row = await cursor.fetchone()
                logger.debug(f"Row fetched: {row}")
                if row:
                    data = {
                        "id": row[0],
                        "name": row[1],
                        "controlfiles": json.loads(await row[2].read()) if row[2] else [],
                        "additionalColumns": json.loads(await row[3].read()) if row[3] else []
                    }
                    logger.debug(f"Data fetched: {data}")
                    return ObjectModel(**data)
                else:
                    logger.warning(f"Object with name {name} not found in the database")
                    return None
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"Object with name {name} not found: {str(e)}")
        return None


async def __get_control_files_by_object_name(object_name: str) -> List[str]:
    """
    Get control files associated with an object name
    
    Args:
        object_name: Name of the object
        
    Returns:
        List[str]: List of control file names
    """
    obj = await __get_object_by_name(object_name)
    if obj:
        return obj.controlfiles
    return []


#Start of main exposed functions

async def get_additional_fields(object_name: str) -> List[str]:
    """
    Retrieve additional fields for a given object.
    This asynchronous function fetches an object by its name and checks if it has 
    any additional columns. If additional columns are found, they are returned as 
    a list of strings. If no additional columns are found or an error occurs, an 
    empty list is returned.
    Args:
        object_name (str): The name of the object to retrieve additional fields for.
    Returns:
        List[str]: A list of additional field names if found, otherwise an empty list.
    Raises:
        None: Any exceptions encountered are logged, and an empty list is returned.
    """
    
    try:
        # Get the object by name
        obj = await __get_object_by_name(object_name)
        
        # Check if the object exists and has additional columns        
        if obj and hasattr(obj, 'additionalColumns'):
            logger.debug(f"Found additional fields for {object_name}: {obj.additionalColumns}")
            return obj.additionalColumns
        
        logger.warning(f"No additional fields found for {object_name}")
        return []
    except Exception as e:
        logger.error(f"Error retrieving additional fields for {object_name}: {str(e)}")
        return []

async def get_control_file_urls_by_object_name(object_name: str) -> List[str]:
    """
    Get full URLs for control files associated with an object name
    
    Args:
        object_name: Name of the object
        
    Returns:
        List[str]: List of control file URLs
    """
    control_files = await __get_control_files_by_object_name(object_name)
    config = await __get_fbdi_config()
    
    urls = []
    for cf in control_files:
        url = f"{config.control_file_prefix}{config.version}{config.control_file_sufix}/{cf}"
        urls.append({cf:url})
    
    return urls

async def get_fbdi_object_names() -> List[str]:
    """
    Retrieve the names of FBDI objects from the database.

    Returns:
        List[str]: A list of FBDI object names.
    """

    conn = await __getConnection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT NAME FROM FBDI_OBJECT")
            rows = await cursor.fetchall()
            object_names = [row[0] for row in rows]
            return object_names
    except Exception as e:
        logger.error(f"Error fetching FBDI object names: {str(e)}")
        return []
    finally:
        await conn.close()

async def execute_sql_query(query: str) -> pd.DataFrame:
    """
    Run a SQL query and return the results as a pandas DataFrame.
    If any column is of type CLOB, convert it to a string.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A DataFrame representing the query results.
        
    """
    
    conn = await __getConnection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(query)
            rows = await cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            
            # Convert CLOB columns to strings
            processed_rows = []
            for row in rows:
                processed_row = []
                for col in row:
                    # Check if the column is a CLOB using hasattr or type comparison
                    if hasattr(col, "read"):  # CLOBs typically have a `read` method
                        processed_row.append(await col.read())
                    else:
                        processed_row.append(col)
                processed_rows.append(processed_row)
            
            df = pd.DataFrame(processed_rows, columns=columns)
            return df
    except Exception as e:
        logger.error(f"Error executing SQL query: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error
    finally:
        await conn.close()




async def main():
    # urls = await get_control_file_urls_by_object_name('APInvoice')
    # print(urls)
    data = await execute_sql_query(query="SELECT * FROM FBDI_OBJECT")
    print(data)

if __name__ == "__main__":
    asyncio.run(main())