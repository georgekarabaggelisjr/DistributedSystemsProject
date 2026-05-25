from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from core.security import SecurityManager
from core.config import settings
from services.manager_router import ManagerRouter
from services.orchestrator import JobOrchestrator
from services.storage_client import StorageClient

# --- Security Setup ---
oauth_scheme = OAuth2PasswordBearer(tokenUrl="token")
security = SecurityManager(settings.KEYCLOAK_PUBLIC_KEY)

async def get_current_user(token: str = Depends(oauth_scheme)) -> dict:
    try:
        payload = security.verify_and_decode_token(token)
        if not payload:
            raise ValueError("Empty token payload")
        return payload
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"Authenticate": "Bearer"},
        )

async def require_admin_role(current_user: dict = Depends(get_current_user)) -> dict:
    realm_access = current_user.get("realm_access", {})
    roles = realm_access.get("roles", [])

    if "admin" not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operation forbidden. Admin privileges required."
        )
    return current_user

# === Service Dependencies ===

def get_storage_client() -> StorageClient:
    return StorageClient(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY
    )

def get_manager_router() -> ManagerRouter:
    return ManagerRouter(
        manager_service_dns=settings.MANAGER_SERVICE_DNS,
        num_replicas=settings.MANAGER_REPLICAS
    )

async def get_orchestrator(
    storage: StorageClient = Depends(get_storage_client),
    router: ManagerRouter = Depends(get_manager_router)
) -> JobOrchestrator:
    return JobOrchestrator(storage=storage, router=router)