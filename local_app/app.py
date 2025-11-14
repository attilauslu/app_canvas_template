import sys
import os
import traceback

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os

# from dotenv import load_dotenv
from pathlib import Path

# # Find .env file
# root_dir = Path(__file__).parent.parent
# dotenv_path = root_dir / '.env'

# # Load environment variables
# load_dotenv(dotenv_path=dotenv_path)

# # Verify the variable is loaded
# app_def_id = os.getenv("APP_DEFINITION_ID")
# if not app_def_id:
#     print("WARNING: APP_DEFINITION_ID is not set!")

from threading import Thread

from benchling_sdk.apps.helpers.webhook_helpers import verify
from flask import Flask, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

from local_app.benchling_app.handler import handle_webhook
from local_app.benchling_app.setup import app_definition_id
from local_app.lib.logger import get_logger

logger = get_logger()


def create_app() -> Flask:
    app = Flask("clc-registration-app")
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    @app.route("/health")
    def health_check() -> tuple[str, int]:
        # Just a route allowing us to check that Flask itself is up and running
        return "OK", 200

    @app.route("/1/webhooks/<path:target>", methods=["POST"])
    def receive_webhooks(target: str) -> tuple[str, int]:
        logger.info(f"Received webhook request for target: {target}")
        try:
            # For security, don't do anything else without first verifying the webhook
            app_def_id = app_definition_id()

            if not app_def_id:
                logger.error("APP_DEFINITION_ID is not set, cannot verify webhook")
                return jsonify({"error": "App configuration error"}), 500

            # Get the raw request body as a string for verification
            raw_body = request.data.decode("utf-8")
            # print("!!!!!!!!!" + raw_body)

            try:
                # Important! To verify webhooks, we need to pass the body as an unmodified string
                verify(app_def_id, raw_body, request.headers)
            except Exception as e:
                logger.error(f"Webhook verification failed: {app_def_id}")
                logger.error(f"Webhook verification failed: {raw_body}")
                logger.error(f"Webhook verification failed: {str(e)}")
                return jsonify({"error": "Webhook verification failed"}), 401

            # Parse JSON only after verification
            try:
                webhook_data = request.json
                logger.info(f"Webhook data: {webhook_data}")
            except Exception as e:
                logger.error(f"Failed to parse webhook JSON: {str(e)}")
                return jsonify({"error": "Invalid JSON in request"}), 400

            # Dispatch work and ACK webhook as quickly as possible
            _enqueue_work(webhook_data)

            # ACK webhook by returning 2xx status code so Benchling knows the app received the signal
            return jsonify({"status": "ok"}), 200

        except Exception as e:
            logger.error(f"Error processing webhook: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": "Internal server error"}), 500

    return app


def _enqueue_work(webhook_data) -> None:
    # PRODUCTION NOTE: A high volume of webhooks may spawn too many threads and lead to processing failures
    # In production, we recommend a more robust queueing system for scale
    try:
        thread = Thread(
            target=handle_webhook,
            args=(webhook_data,),
        )
        thread.start()
        logger.debug("Successfully enqueued webhook for processing")
    except Exception as e:
        logger.error(f"Failed to enqueue work: {str(e)}")
        logger.error(traceback.format_exc())


# if __name__ == "__main__":
#     app = create_app()
#     # Adding more debug output when starting the app
#     logger.info("Starting Flask application on 0.0.0.0:8080")
#     app.run(host='0.0.0.0', port=8080, debug=True)
