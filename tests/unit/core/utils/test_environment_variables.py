import pytest

from app.core.utils.environment_variables import expand_environment_variables


def test_placeholder_is_replaced_at_any_depth(monkeypatch):
    monkeypatch.setenv("CW_PASSWORD", "secret")
    value = {"a": {"credentials": {"password": "${CW_PASSWORD}", "username": "user"}}, "b": ["${CW_PASSWORD}", 3]}
    assert expand_environment_variables(value) == {
        "a": {"credentials": {"password": "secret", "username": "user"}}, "b": ["secret", 3]}


def test_input_is_not_changed(monkeypatch):
    monkeypatch.setenv("X_VAR", "1")
    value = {"a": "${X_VAR}"}
    expand_environment_variables(value)
    assert value == {"a": "${X_VAR}"}


@pytest.mark.parametrize("text", ["plain", "prefix ${X_VAR}", "${X_VAR} suffix", "$X_VAR", "${}", "${1BAD}", 5, None, True])
def test_other_values_are_left_as_they_are(monkeypatch, text):
    monkeypatch.setenv("X_VAR", "1")
    assert expand_environment_variables(text) == text


@pytest.mark.parametrize("set_empty", [False, True])
def test_missing_or_empty_variable_is_an_error_without_values(monkeypatch, set_empty):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    if set_empty:
        monkeypatch.setenv("MISSING_VAR", "")
    with pytest.raises(ValueError, match=r"'MISSING_VAR'.*'a\.b'"):
        expand_environment_variables({"a": {"b": "${MISSING_VAR}"}})
