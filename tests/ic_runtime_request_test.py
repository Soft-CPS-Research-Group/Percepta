from app.utils.data import DataSet
from app.utils.logger import LoggingUtils
from app.ic_runtime_request_2 import ICRuntimeRequest  # adjust import path to your project

# -------- Required configurations --------
environments = {
    "SaoMamede": {
        "group": "SaoMamede",
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
            "port": 5674,
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
        "publisher_settings": {
        "connection": {
          "host": "softcps.dei.isep.ipp.pt",
          "port": 5674,
          "vhost": "/",
          "heartbeat": 600,
          "auth": {
            "username": "dataprovider",
            "password": "dataprovidermq"
          }
        },
        "topology": {
            "exchange_name": "",
            "exchange_properties": {
            },
            "routing_key": "RPC",
            "message_properties": {
              "expiration": "10000"
            }
        },
        "rpc_config": {
          "enabled": True,
          "timeout_ms": 11000,
          "reply_queue": {
            "name": "",
            "exclusive": True,
            "auto_delete": True,
            "durable": False
          }
        }
        }
    },
    "frequency": {
        "value": 5,
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
        time_interval = DataSet.calculate_interval(configurations.get('frequency'))

        ic_runtime = ICRuntimeRequest(["SaoMamede"], configurations, time_interval, logger)
        logger.info("Initializing runtime request...")

        # Start background service (sends request and listens for responses)
        ic_runtime.start_service()
        # Teste 2
        ic_runtime.start_service()

        ic_runtime.stop()

    except Exception as e:
        logger.error(f"Integration test error: {e}")
