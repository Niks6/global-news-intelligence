"""
Global News Intelligence Platform — FastAPI Backend
"""
from __future__ import annotations
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.common.config import get_settings
from backend.routers import health, overview, events, topics, countries, comparison, analytics, map_router, search, regions

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Global News Intelligence API starting up — version 1.0.0")
    logger.info("HBase: %s:%s", settings.hbase_host, settings.hbase_thrift_port)
    yield
    logger.info("API shutting down")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Global News Intelligence API",
        description=(
            "Real-time global news intelligence powered by GDELT. "
            "Provides events, trends, country analytics, sentiment, and anomaly detection."
        ),
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[settings.frontend_url, "http://localhost:3000", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health.router, prefix="/api", tags=["Health"])
    app.include_router(overview.router, prefix="/api", tags=["Overview"])
    app.include_router(events.router, prefix="/api", tags=["Events"])
    app.include_router(topics.router, prefix="/api", tags=["Topics"])
    app.include_router(countries.router, prefix="/api", tags=["Countries"])
    app.include_router(comparison.router, prefix="/api", tags=["Comparison"])
    app.include_router(analytics.router, prefix="/api", tags=["Analytics"])
    app.include_router(map_router.router, prefix="/api", tags=["Map"])
    app.include_router(search.router, prefix="/api", tags=["Search"])
    app.include_router(regions.router, prefix="/api", tags=["Regions"])

    return app


app = create_app()

