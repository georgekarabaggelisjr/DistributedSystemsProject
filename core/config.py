from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # FastAPI
    PROJECT_NAME: str = "MapReduce UI Gateway"
    VERSION: str = "1.0.0"

    # PostgreSQL (DDS)
    DATABASE_URL: str

    # Keycloak Auth
    KEYCLOAK_SERVER_URL: str
    KEYCLOAK_REALM: str
    KEYCLOAK_ADMIN_USER: str
    KEYCLOAK_ADMIN_PASSWORD: str
    KEYCLOAK_PUBLIC_KEY: str

    # MinIO (Shared File System)
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str

    # RabbitMQ
    RABBITMQ_URL: str

    # Internal Routing
    MANAGER_SERVICE_DNS: str = "manager-service.default.svc.cluster.local"
    MANAGER_REPLICAS: int = 3

    class Config:
        env_file = ".env"
        case_sensitive = True

# Instantiate a global settings object to be imported by other modules
settings = Settings()