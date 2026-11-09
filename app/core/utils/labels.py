from enum import Enum


class InputHandlerLabel(Enum):
    """Ids of the types of Input Handler, used to register them in the Input Handlers Launcher and to ask for one.

    They are also the 'protocol_id' of the connection configurations (e.g., of a Telemetry System).
    """
    ASYNC_RABBITMQ = "AsyncRabbitMQ"
    ASYNC_HTTP = "AsyncHTTP"
    ASYNC_LOCAL = "AsyncLocal"


class OutputHandlerLabel(Enum):
    """Ids of the types of Output Handler, used to register them in the Output Handlers Launcher and to ask for one.

    They are also the 'protocol_id' of the connection configurations (e.g., of the inter-container communication).
    """
    ASYNC_RABBITMQ = "AsyncRabbitMQ"
    ASYNC_HTTP = "AsyncHTTP"
    ASYNC_LOCAL = "AsyncLocal"


class SchemaStorageLabel(Enum):
    """Ids of the types of Schema Storage, used to register them in the Schema Storage Launcher and to ask for one.

    They are also the 'schema_storage_type_id' of the Percepta Instance Configuration.
    """
    JSON = "json"


class RuntimeDataStorageLabel(Enum):
    """Ids of the types of Runtime Data Storage, used to register them in the Runtime Data Storage Launcher and to ask
    for one.

    They are also the 'runtime_data_storage_type_id' of the Percepta Instance Configuration.
    """
    MONGODB = "mongodb"


class DataIssueHandlingApproachLabel(Enum):
    """Ids of the types of Data-Issue Handling Approach, used to register them in the Data-Issue Handling Approaches
    Launcher and to ask for one.

    They are also the values of the 'data_issue_handling_approaches' list of the Percepta Instance Configuration.
    The approaches are applied in the order in which they are listed there.
    """
    MISSING_DATA = "missing_data"
    DUPLICATED_DATA = "duplicated_data"
    OUT_OF_ORDER_DATA = "out_of_order_data"
    FREQUENCY_HARMONISATION = "frequency_harmonisation"
