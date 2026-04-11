class SecurityManager:
    def __init__(self, keycloak_public_key):
        """
        Initializes the security manager with the Keycloak Realm's public key.
        """
        pass

    def verify_and_decode_token(self, token: str) -> dict:
        """
        Verifies the JWT signature, checks expiration, and extracts user claims.

        Args:
            token (str): The Bearer token extracted from the Authorization header.

        Returns:
            dict: The decoded token payload containing 'sub' (user_id) and 'roles'.

        Raises:
            HTTPException(401): If the token is expired, tampered with, or invalid.
        """
        pass