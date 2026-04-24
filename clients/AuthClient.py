from core.LocalStateManager import LocalStateManager
import requests


class AuthClient:
    """
    Handles communication with the Keycloak Auth Service for identity management.

    Direct Access Grant:
    This client bypasses the UI Service and communicates directly with Keycloak to
    verify user credentials and retrieve a JWT token, adhering to the Single Sign-On
    (SSO) architecture.
    """

    def __init__(self, state_manager: LocalStateManager):
        self.state_manager = state_manager

    def login(self, username: str, password: str) -> bool:
        url = "http://localhost:8080/realms/myrealm/protocol/openid-connect/token"
        payload = {
            "client_id": "mapreduce-client",
            "username": username,
            "password": password,
            "grant_type": "password",
        }

        try:
            response = requests.post(url, data=payload)
            if response.status_code == 200:
                token_data = response.json()
                self.state_manager.save_token(token_data.get("access_token"))
                return True
            else:
                # ΔΕΣ ΕΔΩ: Εκτύπωσε την απάντηση του Keycloak για να ξέρουμε γιατί αρνείται
                print(f"DEBUG: Keycloak responded with {response.status_code}: {response.text}")
                return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False