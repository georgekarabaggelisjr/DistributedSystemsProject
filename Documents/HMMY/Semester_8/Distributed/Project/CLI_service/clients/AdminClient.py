from core.LocalStateManager import LocalStateManager

import requests
import typer
from core.LocalStateManager import LocalStateManager

class AdminClient:
    """
    Handles elevated operations restricted to system administrators.

    If the provided JWT does not contain the 'admin' role in its claims,
    the UI service will reject these requests with a 403 Forbidden.
    """

    def __init__(self, state_manager: LocalStateManager):
        self.state_manager = state_manager
        self.base_url = self.state_manager.get_ui_service_url()

    def _get_auth_header(self):
        token = self.state_manager.get_token()
        if not token:
            typer.secho("❌ Error: Admin session expired. Please login.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def create_user(self, user_details: dict) -> str:
        """
        Executes the `admin users create` workflow.

        Sends a JSON payload via HTTP POST to `/admin/users`. The UI Service
        acts as a proxy to the Keycloak Admin REST API to provision the user.
        Expects a 201 Created response.

        Args:
            user_details (dict): A dictionary containing username, email, and password.

        Returns:
            str: The newly created Keycloak User ID.
        """
        url = f"{self.base_url}/admin/users"
        headers = self._get_auth_header()

        try:
            # Αποστολή των στοιχείων του χρήστη ως JSON
            response = requests.post(url, headers=headers, json=user_details)

            # Αν ο χρήστης δεν είναι admin, το UI Service θα γυρίσει 403 Forbidden
            if response.status_code == 403:
                typer.secho("🚫 Access Denied: Admin privileges required.", fg=typer.colors.RED)
                raise typer.Exit(code=1)

            response.raise_for_status()

            # Επιστροφή του Keycloak User ID
            data = response.json()
            return data.get("id", "User created successfully")

        except requests.exceptions.RequestException as e:
            typer.secho(f"❌ Failed to create user: {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

    def configure_workers(self, worker_count: int) -> bool:
        """
        Executes the `admin config workers --count <N>` workflow.

        Sends an HTTP POST to `/admin/config` with the new system limits.
        The UI service updates the PostgreSQL DDS and broadcasts a 'ConfigUpdated'
        event via RabbitMQ's Fanout Exchange to all active Manager Replicas.

        Args:
            worker_count (int): The maximum number of workers the system should scale to.

        Returns:
            bool: True if the configuration was successfully applied and broadcasted.
        """
        url = f"{self.base_url}/admin/config"
        headers = self._get_auth_header()

        # Το payload που περιμένει το FastAPI (βλ. ConfigUpdateRequest στα schemas)
        payload = {"max_workers": worker_count}

        try:
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 403:
                typer.secho("🚫 Access Denied: Cannot modify system configuration.", fg=typer.colors.RED)
                return False

            response.raise_for_status()
            return True

        except requests.exceptions.RequestException as e:
            typer.secho(f"❌ Configuration broadcast failed: {e}", fg=typer.colors.RED)
            return False