from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from core.security import SecurityManager
from core.config import settings

# Defines where the token is extracted from (The 'Authorization' header)
oauth_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Logic to validate JWTs via the SecurityManager class
security = SecurityManager(settings.KEYCLOAK_PUBLIC_KEY)


async def get_db():
    """
    Dependency: Provides an asynchronous session to the PostgreSQL DDS.
    Ensures that the connection is opened before the request and closed after.
    """
    pass


async def get_current_user(token: str = Depends(oauth_scheme)) -> dict:
    """
    Dependency: The primary security gatekeeper for all standard routes.

    1. Calls security.verify_and_decode_token().
    2. If valid, returns the user claims (sub, username, roles).
    3. If invalid/expired, raises HTTP 401.
    """
    pass


async def require_admin_role(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Dependency: Chained with get_current_user to enforce Admin-only access.

    1. Checks the 'roles' list in the decoded JWT payload.
    2. If 'admin' is missing, raises HTTP 403 Forbidden.
    """
    pass