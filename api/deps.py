from typing import AsyncGenerator
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from core.security import SecurityManager
from core.config import settings

# --- Database Setup ---
# 1. Δημιουργούμε τον κινητήρα (engine) που μιλάει με την PostgreSQL ασύγχρονα
engine = create_async_engine(settings.DATABASE_URL, echo=False)

# 2. Φτιάχνουμε το εργοστάσιο παραγωγής sessions (Session Factory)
async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# --- Security Setup ---
# Defines where the token is extracted from (The 'Authorization' header)
oauth_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Logic to validate JWTs via the SecurityManager class
security = SecurityManager(settings.KEYCLOAK_PUBLIC_KEY)



async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency: Provides an asynchronous session to the endpoint for communicating with the PostgreSQL DDS.
    Ensures that the connection is opened before the request and closed after.
    """
    async with async_session_maker() as session:
        yield session


async def get_current_user(token: str = Depends(oauth_scheme)) -> dict:
    """
    Dependency: The primary security gatekeeper for all standard routes.

    1. Calls security.verify_and_decode_token().
    2. If valid, returns the user claims (sub, username, roles).
    3. If invalid/expired, raises HTTP 401.
    """
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
    """
    Dependency: Chained with get_current_user to enforce Admin-only access.

    1. Checks the 'roles' list in the decoded JWT payload.
    2. If 'admin' is missing, raises HTTP 403 Forbidden.
    """
    realm_access = current_user["realm_access", {}]
    roles = realm_access.get("roles", [])

    if "admin" not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operation forbidden. Admin privileges required."
        )

    return current_user