"""
Authentication and authorization dependency module.

This module provides FastAPI dependencies for securing internal API endpoints
using JSON Web Tokens (JWT). It is engineered to integrate seamlessly with
enterprise Identity Providers (IdPs) like Keycloak, enforcing strict
cryptographic validation of the token's RS256 signature and evaluating
standard claims such as expiration and audience.
"""

import logging
from typing import Optional, Dict, Any
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.core.config import settings

# Initialize module-level logger for security audit events
logger = logging.getLogger(__name__)

# FastAPI security scheme that automatically inspects the Authorization header.
# auto_error is set to False to permit graceful programmatic short-circuiting
# under automated local testing environments.
security = HTTPBearer(
    description="Provide a valid Keycloak JWT token to access this API.",
    auto_error=False
)

async def verify_jwt_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Dict[str, Any]:
    """
    Validates the incoming JWT and extracts the decoded payload.

    This foundational dependency intercepts the HTTP request, extracts the Bearer
    token from the Authorization header, and cryptographically verifies its
    signature against the configured Public Key. If ENABLE_AUTH is false,
    it completely bypasses verification and falls back onto a standardized
    development payload to support headless integration testing scripts.

    Args:
        credentials (Optional[HTTPAuthorizationCredentials]): The credentials object
            automatically injected by FastAPI's HTTPBearer scheme. Can be None
            if the client sent no Authorization header.

    Returns:
        Dict[str, Any]: The secure, cryptographically verified payload dictionary,
            or a hardcoded development fallback context.

    Raises:
        HTTPException: Raises a 401 Unauthorized error with specific detail messages if
            authentication is enabled and verification fails or credentials are missing.
    """
    # --- TESTING PROFILED BYPASS BOUNDARY ---
    if not settings.ENABLE_AUTH:
        logger.warning(
            "CRITICAL SECURITY NOTICE: Authentication validation is disabled via configuration! "
            "Falling back into development tenant isolation and service context."
        )
        # Yield the exact sandbox ID used by local end-to-end and multi-tenant scripts,
        # while also spoofing the UI service identity for internal route testing.
        return {
            "sub": "22222222-2222-2222-2222-222222222222",
            "clientId": "ui-service",
            "azp": "ui-service"
        }

    # Enforce token presence if auth flag is active
    if credentials is None:
        logger.warning("Rejected anonymous request: Missing required HTTP Authorization Bearer token header.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Missing Bearer token."
        )

    token = credentials.credentials
    public_key = (
        f"-----BEGIN PUBLIC KEY-----\n"
        f"{settings.JWT_PUBLIC_KEY}\n"
        f"-----END PUBLIC KEY-----"
    )

    try:
        # Decode and strictly verify the payload
        payload = jwt.decode(
            jwt=token,
            key=public_key,
            algorithms=[settings.JWT_ALGORITHM],
            audience=settings.JWT_AUDIENCE,
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_aud": True
            }
        )
        return payload

    except jwt.ExpiredSignatureError:
        logger.warning("Authentication rejected: JWT token has expired.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired. Please refresh your credentials."
        )
    except jwt.InvalidTokenError as e:
        logger.warning("Authentication rejected: Cryptographic verification failed. Reason: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token."
        )
    except Exception as e:
        logger.error("Critical failure during JWT verification: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during authentication processing."
        )


async def get_current_user_id(payload: Dict[str, Any] = Depends(verify_jwt_token)) -> str:
    """
    Extracts the authenticated User ID from a verified JWT payload.

    This dependency builds upon `verify_jwt_token` to specifically retrieve
    the subject representing the end-user or tenant making the request.

    Args:
        payload (Dict[str, Any]): The cryptographically verified JWT payload.

    Returns:
        str: The secure identifier of the authenticated tenant, extracted
            from the 'sub' (subject) claim.

    Raises:
        HTTPException: Raises 401 if the subject claim is missing from the payload.
    """
    user_id = payload.get("sub")
    if user_id is None:
        logger.error("Authentication failed: JWT verified, but 'sub' claim is missing.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token structure: missing subject claim (sub)."
        )

    return str(user_id)


async def verify_ui_service(payload: Dict[str, Any] = Depends(verify_jwt_token)) -> Dict[str, Any]:
    """
    Dependency that verifies the caller is strictly the internal UI Service.

    This acts as a machine-to-machine boundary, ignoring end-user context
    and ensuring the request originates specifically from the allowed internal
    microservice by inspecting the client identity claims.

    Args:
        payload (Dict[str, Any]): The cryptographically verified JWT payload.

    Returns:
        Dict[str, Any]: The valid payload, passed through for downstream use if authorized.

    Raises:
        HTTPException: Raises 403 Forbidden if the token identity does not match
            the UI service signature.
    """
    # Keycloak typically stores the Machine-to-Machine client identifier
    # in 'clientId' or 'azp' (Authorized Party).
    client_id = payload.get("clientId") or payload.get("azp")

    if client_id != "ui-service":
        logger.warning("Blocked unauthorized service attempt from client ID: %s", client_id)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden. Only the UI service can perform this action."
        )

    return payload