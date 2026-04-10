from core.LocalStateManager import LocalStateManager


class AuthClient:
    """
    Handles communication with the Keycloak Auth Service for identity management.

    Direct Access Grant:
    This client bypasses the UI Service and communicates directly with Keycloak to
    verify user credentials and retrieve a JWT token, adhering to the Single Sign-On
    (SSO) architecture.
    """

    def __init__(self, state_manager: LocalStateManager):
        pass

    def login(self, username: str, password: str) -> bool:
        """
        Executes the `login <credentials>` workflow.

        Issues an HTTP POST request to the Auth Service's `/token` endpoint.
        Upon a 200 OK response, it extracts the JWT and uses the LocalStateManager
        to persist it. If it receives a 401 Unauthorized, it raises a structured error.

        Args:
            username (str): The user's registered Keycloak username.
            password (str): The user's password.

        Returns:
            bool: True if authentication is successful, False otherwise.
        """

        # Mock
        if username == "iason" and password == "1":
            return True
        else:
            False