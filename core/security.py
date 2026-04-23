import jwt
from fastapi import HTTPException, status

class SecurityManager:

    def __init__(self, keycloak_public_key): # Getting the public key from keycloak
        """
        Initializes the security manager with the Keycloak Realm's public key.

        NOTES:  1) The JWT is raw (not encrypted) and signed. It contains user_id, username, role, expiration
                2) Keycloak just provides the JWT (MIIB..), but the jwt library requires a kind of 'header'
        """
        if not keycloak_public_key.startswith("-----BEGIN PUBLIC KEY-----"): # Need to build the 'header'
            self.public_key = (
                f"-----BEGIN PUBLIC KEY-----\n"
                f"{keycloak_public_key}\n"
                f"-----END PUBLIC KEY-----"
            )
        else: # 'Header' is already fine
            self.public_key = keycloak_public_key

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
        try:
            payload = jwt.decode(
                token,
                self.public_key,
                algorithms=["RS256"], # Keycloak algorithm
                # Μπορείς να προσθέσεις options αν θέλεις να ελέγχεις το audience (aud)
                options={"verify_aud": False}
            )
            return payload

        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Το token έχει λήξει. Παρακαλώ συνδεθείτε ξανά."
            )
        except jwt.InvalidTokenError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Μη έγκυρο token: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Σφάλμα κατά την επαλήθευση της ταυτότητας."
            )

    def extract_user_id(self, payload: dict) -> str:
        """
        Extracts the 'sub' (Subject), which at Keycloak it's the user's UUID.
        """
        return payload.get("sub")

    def get_username(self, payload: dict) -> str:
        """
        Extracts the username from the token (usually 'preferred_username').
        """
        return payload.get("preferred_username")