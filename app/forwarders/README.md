# 🧠 Forwarder - Implementation Guide

## ✅ Purpose

A **Forwarder** is responsible for **sending processed results** (e.g., the output of an AI model) to the appropriate destination — such as a database, message queue, remote service, etc.

---

## 🗂 Project Structure

Forwarders are discovered automatically using the `discover_subclasses` function. For this to work properly, **each forwarder must follow a specific naming and structural convention**.

---

## 📦 Expected Structure of a Forwarder

### 1. File name

The file name must follow this pattern:

```
<provider>_forwarder.py
```

**Valid examples:**
- `mqtt_forwarder.py`
- `s3_forwarder.py`
- `database_forwarder.py`

> This ensures that the forwarder is automatically discovered and registered.

---

### 2. Base class

All forwarders must inherit from the abstract class `ForwarderBase`, which defines the minimal required interface:

```python
from abc import ABC, abstractmethod

class ForwarderBase(ABC):
    provider: str  # Unique identifier of the forwarder

    def __init__(self, configurations, logger):
        self._configurations = configurations
        self._logger = logger

    @abstractmethod
    def to_forward(self, result):
        """
        Sends the processed result to the appropriate destination.

        Parameters:
            result: the output object from the model or analysis
        """
        raise NotImplementedError()
```

---

### 3. Required attribute: `provider`

Each forwarder class must declare a class attribute `provider`, which will be used as a **unique key** in the registry:

```python
class MqttForwarder(ForwarderBase):
    provider = "mqtt"

    def to_forward(self, result):
        # logic to publish the result to an MQTT broker
        pass
```

---

## 🔧 Automatic Registration

All valid forwarders are automatically registered using:

```python
FORWARDER_REGISTRY = discover_subclasses(
    package="app.forwarders",
    base_class=ForwarderBase,
    required_suffix="_forwarder"
)
```

They are instantiated using shared dependencies (e.g., configuration and logger):

```python
def build_forwarders(configurations, logger):
    ...
```

---