import time
from app.utils.logger import LoggingUtils
from app.ic_runtime_request import ICRuntimeRequest  # adjust import path to your project

# -------- Required configurations --------
environments = {
    "i-charging headquarters 3Phase": {
        "group": "i-charging headquarters 3Phase",
        "entities": {
            "AC000001_1": {
                "parameters": {
                    "power": {"measurementUnit": "Kilowatt"}
                },
                "label": "ev_charger",
                "serialNumber": "AC000001",
                "plug": 1
            },
            "AC000002_1": {
                "parameters": {
                    "power": {"measurementUnit": "Kilowatt"}
                },
                "label": "ev_charger",
                "serialNumber": "AC000002",
                "plug": 1
            },
        }
    }
}

configurations = {
    "i-charging": {
        "type": "amqp",
        "receiver_server": {
            "host": "softcps.dei.isep.ipp.pt",
            "port": 5672,
            "heartbeat": 660,
            "auth": {
                "username": "dataprovider",
                "password": "dataprovidermq"
            },
            "exchange_conf": {
                "durable": False,
                "exchange_type": "fanout"
            },
            "queue_conf": {
                "durable": True
            },
            "consume_conf": {
                "auto_ack": False
            }
        },
    },
    "frequency": {
        "value": 0,
        "unit": "seconds"
    },
    "log_files": {
        "max_size": 10485760
    }
}

# -------- Logger --------
logger = LoggingUtils("ic_runtime_request_test", configurations)

# -------- Integration Test --------
if __name__ == "__main__":
    try:
        ic_runtime = ICRuntimeRequest(["i-charging headquarters 3Phase"], configurations, logger)
        logger.info("Initializing runtime request...")

        # Start background service (sends request and listens for responses)
        ic_runtime.start_service()

    except Exception as e:
        logger.error(f"Integration test error: {e}")
