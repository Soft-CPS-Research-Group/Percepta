from unittest.mock import MagicMock

import pytest

from app.core.adapters.ml_model_adapter_base import MLModelAdapterBase


class _Adapter(MLModelAdapterBase):
    def adapt_model_input(self, message):
        return message

    def adapt_model_decisions(self, decisions):
        return []


def test_cannot_be_instantiated_without_the_two_methods():
    class _Incomplete(MLModelAdapterBase):
        def adapt_model_input(self, message):
            return message

    with pytest.raises(TypeError):
        _Incomplete("env", {}, MagicMock())


def test_valid_initialization():
    adapter = _Adapter("env", {"entities": {}}, MagicMock())
    assert adapter.adapt_model_input({"a": 1}) == {"a": 1}
    assert adapter.adapt_model_decisions({}) == []


@pytest.mark.parametrize("name,environment", [("", {}), ("  ", {}), (None, {}), ("env", None), ("env", [])])
def test_invalid_initialization(name, environment):
    with pytest.raises(ValueError):
        _Adapter(name, environment, MagicMock())


def test_missing_logger():
    with pytest.raises(ValueError):
        _Adapter("env", {}, None)
