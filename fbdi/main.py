# main.py - Main FastAPI application entry point
import uvicorn
from fastapi import FastAPI, Depends , HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fbdi.control_file.api.routes import router as control_router
from asgi_correlation_id import CorrelationIdMiddleware
from fastapi.exception_handlers import http_exception_handler
from contextlib import asynccontextmanager
from fbdi.utils.logging_conf import configure_logger
from fbdi.client.schema import router as schema_router

from fbdi.generator.api.routes import router as transform_routrer
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app:FastAPI):
    configure_logger()
    yield    

# print(__name__)    

# Initialize FastAPI app
app = FastAPI(
    title="Oracle FBDI Automation API",
    description="API for automating Oracle FBDI control file processing without local storage",
    version="1.0.0",
    lifespan=lifespan
)


# Add CORS middleware
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins - change for production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include API routes
app.include_router(control_router, prefix="/api/v1")
# Add this line along with the other app.include_router calls
app.include_router(schema_router, prefix="/api/v1/client/schema")
# Add transform router to generate FBDI zip file
app.include_router(transform_routrer , prefix="/api/v1/fbdi")

# Health check endpoint
# @app.get("/health")
# async def health_check():
#     return {"status": "healthy", "version": "1.0.0"}




@app.exception_handler(HTTPException)
async def http_exception_hadler_logging(request , exec):
    logger.error(f"Exception details {exec.status_code} - {exec.detail}")
    return await http_exception_handler(request , exec)

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)