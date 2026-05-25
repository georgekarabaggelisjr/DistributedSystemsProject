import json
from fastapi import APIRouter, Depends, Body, HTTPException, status
from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakError
import aio_pika

from api.deps import require_admin_role
from models.schemas import UserCreateSchema, ConfigUpdateSchema
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
        raise HTTPException(status_code=400, detail=f"Keycloak error: {str(e)}")
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to create user.")

@router.post("/config")
async def update_system_limit(
    config: ConfigUpdateSchema = Body(...),
    admin_user: dict = Depends(require_admin_role)
):
    """
    Stateless Admin Config:
    Broadcasting the configuration update via RabbitMQ.
    The active backend Managers will handle persistence and real-time synchronization.
    """
    try:
        # Σύνδεση στο RabbitMQ απευθείας (Stateless Broadcast)
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)

        async with connection:
            channel = await connection.channel()

            exchange = await channel.declare_exchange(
                name="manager_config_updates",
                type=aio_pika.ExchangeType.FANOUT
            )

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

        return {"message": f"Configuration '{config.key}' broadcasted to MapReduce cluster successfully!"}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to broadcast configuration: {str(e)}"
        )