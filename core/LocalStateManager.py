import json
import os
from pathlib import Path

class LocalStateManager:

    """
    Manages the local configuration and authentication state for the CLI application.

    Persistent State:
    To avoid requiring the user to authenticate before every command, this class
    manages a local configuration file (e.g., `~/cli_config.json`).
    It handles the secure storage of the Keycloak JWT token and the base URLs
    for the API Gateway (UI Service).
    """


    def __init__(self, config_file: str = ".mapreduce_auth_config.json"):
        """
        Initializes the state manager, ensuring the local configuration directory exists.

        We store the file in the user's home directory so it's accessible
        from any terminal location.
        """

        self.config_path = Path.home() / config_file
        self._ensure_config_exists()

        pass

    def _ensure_config_exists(self):
        """Creates an empty JSON configuration file if it doesn't exist."""
        if not self.config_path.exists():
            default_config = {
                "jwt_token": None,
                "ui_service_url": "http://localhost:8000",
            }
            self._write_config(default_config)

    def _write_config(self, config: dict):
        """Write the JSON configuration file."""
        with open(self.config_path, "w") as f:
            json.dump(config, f, indent=4)

    def _read_config(self) -> dict:
        """Reads the JSON configuration file."""
        try:
            with open(self.config_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"jwt_token": None, "ui_service_url": "http://ui-service.192.168.49.2.nip.io"}

    def save_token(self, jwt_token: str) -> None:
        """
        Saves the Keycloak JWT token to the local configuration file.

        Args:
            jwt_token (str): The encrypted JWT issued by the Auth Service.
        """
        config = self._read_config()
        config["jwt_token"] = jwt_token
        self._write_config(config)
        pass

    def get_token(self) -> str:
        """
        Retrieves the locally stored JWT token.
        """
        config = self._read_config()
        return config.get("jwt_token")

    def get_ui_service_url(self) -> str:
        """
        Retrieves the base URL for the UI Service (API Gateway).
        First looks for an environment variable, then the config file.
        """
        # Pro tip: Check Environment Variable first (good for Docker/Testing)
        env_url = os.getenv("UI_SERVICE_URL")
        if env_url:
            return env_url

        config = self._read_config()
        return config.get("ui_service_url", "http://localhost:8000")

    def logout(self) -> None:
        """Clears the token from the config"""
        self.save_token(None)