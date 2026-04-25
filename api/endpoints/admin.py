import json
from fastapi import APIRouter, Depends, Body, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakError
import aio_pika

from api.deps import require_admin_role, get_db
from models.schemas import UserCreateSchema, ConfigUpdateSchema
from models.dds_models import SystemConfigRecord # Το ORM model για τον πίνακα system_config
from core.config import settings

router = APIRouter()

keycloak_admin = KeycloakAdmin(
    server_url=settings.KEYCLOAK_SERVER_URL,
    username=settings.KEYCLOAK_ADMIN_USER,
    password=settings.KEYCLOAK_ADMIN_PASSWORD,
    realm_name=settings.KEYCLOAK_REALM,
    user_realm_name="master",
    verify=True
)

@router.post("/users", status_code=201)
async def create_user(
    user_data: UserCreateSchema = Body(...),
    admin_user: dict = Depends(require_admin_role)
):
    """
    REST Endpoint for CLI: 'admin users-create'.
    
    1. Authenticates against Keycloak using Admin credentials.
    2. Provisions a new user in the specific Realm.
    3. Returns the generated Keycloak UUID.
    """
    try:
        kc_user_payload = {
            "email": user_data.email,
            "username": user_data.username,
            "enabled": True,
            "credentials": [{"value": user_data.password, "type": "password"}]
        }

        new_user_id = keycloak_admin.create_user(kc_user_payload)

        return {
            "message": "User provisioned in Keycloak successfully",
            "keycloak_id": new_user_id
        }
    except KeycloakError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Keycloak error: {str(e)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user."
        )

@router.post("/config")
async def update_system_limit(
    config: ConfigUpdateSchema = Body(...),
    admin_user: dict = Depends(require_admin_role),
    db = Depends(get_db)
):
    """
    REST Endpoint for CLI: 'admin config-workers'.
    
    1. Updates the worker limits in the PostgreSQL 'system_settings' table.
    2. Triggers the MessageBroker to broadcast a Fanout message to all Manager replicas.
    """
    try:
        # Update the DDS
        stmt = (
            update(SystemConfigRecord)
            .where(SystemConfigRecord.config_key == config.key)
            .values(config_value=str(config.value))
        )
        result = await db.execute(stmt)

        # Check that this config really exists in DDS
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Config key '{config.key}' not found.")

        await db.commit()

        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)

        async with connection:
            channel = await connection.channel()

            # Connect to Fanout Exchange
            exchange = await channel.declare_exchange(
                name="manager_config_updates",
                type=aio_pika.ExchangeType.FANOUT
            )

            # Send (Broadcast)
            message_body = json.dumps({
                "event": "ConfigUpdated",
                "key": config.key,
                "new_value": config.value,
                "updated_by": admin_user["sub"]
            }).encode()

            message = aio_pika.Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await exchange.publish(message, routing_key="")

        return {"message": f"Configuration '{config.key}' updated to {config.value} and broadcasted successfully!"}


    except Exception as e:

        await db.rollback()

        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update configuration: {str(e)}"
        )