import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.endpoints import jobs, admin
from core.config import settings


def create_app() -> FastAPI:
    """
    Initialize FastAPI App and connection of the subsystems.
    """
    app = FastAPI(
        title=settings.PROJECT_NAME,
        description="Distributed Map-Reduce UI Gateway API",
        version="1.0.0"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Σύνδεση των Routers
    # prefix: η διαδρομή URL (π.χ. /jobs/submit)
    # tags: για την κατηγοριοποίηση στο αυτόματο documentation (/docs)
    app.include_router(
        jobs.router,
        prefix="/jobs",
        tags=["Jobs Management"]
    )

    app.include_router(
        admin.router,
        prefix="/admin",
        tags=["Admin Operations"]
    )

    @app.get("/", tags=["Health"])
    async def root():
        """
        Health check endpoint to ensure that the UI Service is running.
        """
        return {
            "service": settings.PROJECT_NAME,
            "status": "online",
            "version": "1.0.0"
        }

    return app


app = create_app()

if __name__ == "__main__":
    # Start Uvicorn server
    # host 0.0.0.0: So that the UI_service is seen from inside the Docker/Kubernetes
    # port 8000: default
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        # TODO: Change it to False when ready
        reload=True  # Only for development phase
    )