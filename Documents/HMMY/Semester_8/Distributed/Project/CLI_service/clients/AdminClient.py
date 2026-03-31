from core.LocalStateManager import LocalStateManager


class AdminClient:
    """
    Handles elevated operations restricted to system administrators.

    If the provided JWT does not contain the 'admin' role in its claims,
    the UI service will reject these requests with a 403 Forbidden.
    """

    def __init__(self, state_manager: LocalStateManager):
        pass

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
        pass

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
        pass