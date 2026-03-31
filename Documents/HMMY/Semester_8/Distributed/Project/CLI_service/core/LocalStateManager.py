class LocalStateManager:
    """
    Manages the local configuration and authentication state for the CLI application.

    Persistent State:
    To avoid requiring the user to authenticate before every command, this class
    manages a local configuration file (e.g., `~/cli_config.json`).
    It handles the secure storage of the Keycloak JWT token and the base URLs
    for the API Gateway (UI Service).

    Author: Kourmoulis Iason
    A.M. 2022030107
    """


    def __init__(self, config_dir: str = "..."):
        """
        Initializes the state manager, ensuring the local configuration directory exists.

        Args:
            config_dir (str): The path to the local directory where config is stored.
        """
        pass

    def save_token(self, jwt_token: str) -> None:
        """
        Saves the Keycloak JWT token to the local configuration file.

        Args:
            jwt_token (str): The encrypted JWT issued by the Auth Service.
        """
        pass

    def get_token(self) -> str:
        """
        Retrieves the locally stored JWT token.

        Returns:
            str: The JWT token, or None if the user is not authenticated.
        """
        pass

    def get_ui_service_url(self) -> str:
        """
        Retrieves the base URL for the UI Service (API Gateway).

        Returns:
            str: The base URL (e.g., 'http://localhost:8080').
        """
        pass