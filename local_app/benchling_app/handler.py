from typing import Any

from benchling_sdk.apps.status.errors import AppUserFacingError
from benchling_sdk.models.webhooks.v0 import (
    CanvasCreatedWebhookV2Beta,
    CanvasInitializeWebhookV2,
    CanvasInteractionWebhookV2,
    WebhookEnvelopeV0,
    CanvasCreatedWebhookV2,
)
import os
from local_app.benchling_app.canvas_interaction import route_interaction_webhook
from local_app.benchling_app.setup import init_app_from_webhook
from local_app.benchling_app.views.canvas_initialize import (
    render_text_canvas,
    render_text_canvas_for_created_canvas,
)
from local_app.lib.logger import get_logger

logger = get_logger()


class UnsupportedWebhookError(Exception):
    pass


def handle_webhook(webhook_dict: dict[str, Any]) -> None:
    logger.debug("Handling webhook with payload: %s", webhook_dict)
    webhook = WebhookEnvelopeV0.from_dict(webhook_dict)
    app = init_app_from_webhook(webhook)
    try:
        if isinstance(webhook.message, CanvasInitializeWebhookV2):
            render_text_canvas(app, webhook.message)

        elif isinstance(webhook.message, CanvasInteractionWebhookV2):
            route_interaction_webhook(app, webhook.message)
            print(webhook.message)

        # Support both stable and beta, just in case:
        elif isinstance(webhook.message, (CanvasCreatedWebhookV2, CanvasCreatedWebhookV2Beta)):
            render_text_canvas_for_created_canvas(app, webhook.message)

        else:
            raise UnsupportedWebhookError(
                f"Received an unsupported webhook type: {webhook}"
            )

        logger.debug("Successfully completed request for webhook: %s", webhook_dict)

    except AppUserFacingError as e:
        logger.debug("Exiting with client error: %s", e)

