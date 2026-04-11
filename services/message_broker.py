class MessageBroker:

    async def broadcast_config_update(self, max_workers: int) -> None:
        """
        Publishes a 'ConfigUpdated' event to a RabbitMQ Fanout Exchange.
        This broadcasts the new worker limits to all active Manager replicas simultaneously.
        """
        pass