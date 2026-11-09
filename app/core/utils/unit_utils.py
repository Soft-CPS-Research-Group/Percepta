# --- Standard Library Imports ---
from typing import Any, Callable, Dict, Optional, Tuple

# The families of units that can be converted into each other, and how many powers of ten each unit is worth, counted
# from the base unit of its family (the one with exponent 0). Only the units of the same family can be converted.
# Example: in "power", "W" is 10^-3 of the base unit "kW", and "MW" is 10^3 of it. So 1 MW = 1000 kW = 1 000 000 W.
# The names are compared without caring about upper or lower case, and without spaces around them (e.g., "kwh" is the
# same unit as "kWh").
_FAMILIES: Dict[str, Dict[str, int]] = {
    "power": {"w": -3, "kw": 0, "mw": 3},
    "energy": {"wh": -3, "kwh": 0, "mwh": 3},
    "price": {"€/kwh": 0, "€/mwh": -3},
    "proportion": {"%": -2, "ratio": 0},      # "ratio" is a proportion from 0 to 1, "%" is the same from 0 to 100
}


def _normalise(unit: Any) -> str:
    """Turns a unit into the form used to look it up: text, without spaces around it, in lower case.

    Args:
        unit (Any): The unit, e.g., "kWh".

    Returns:
        str: The unit in the lookup form, e.g., "kwh". It is an empty text if the unit is not a text.
    """
    return unit.strip().lower() if isinstance(unit, str) else ""


def unit_family(unit: Any) -> Optional[str]:
    """Tells the family of a unit.

    Args:
        unit (Any): The unit, e.g., "kWh".

    Returns:
        Optional[str]: "power", "energy", "price" or "proportion", or None if the unit is not one that can be converted
            (e.g., "Status").
    """
    name = _normalise(unit)
    for family, units in _FAMILIES.items():
        if name in units:
            return family
    return None


def _exponents(from_unit: Any, to_unit: Any) -> Tuple[int, int]:
    """Finds the powers of ten of two units that can be converted into each other.

    Args:
        from_unit (Any): The unit of the value.
        to_unit (Any): The unit that the value must have.

    Returns:
        Tuple[int, int]: The exponent of each unit in its family (see `_FAMILIES`).

    Raises:
        ValueError: If a unit is not known, or the two units are not of the same family (e.g., "kW" and "kWh").
    """
    family = unit_family(from_unit)

    # --- Guard Clause 1: both units must be known ---
    if family is None or unit_family(to_unit) is None:
        raise ValueError(
            f"cannot convert from {from_unit!r} to {to_unit!r}: the units that can be converted are "
            f"{', '.join(sorted(name for units in _FAMILIES.values() for name in units))}."
        )

    # --- Guard Clause 2: both units must be of the same family ---
    if unit_family(to_unit) != family:
        raise ValueError(
            f"cannot convert from {from_unit!r} to {to_unit!r}: they are not the same kind of unit "
            f"({family} and {unit_family(to_unit)})."
        )

    return _FAMILIES[family][_normalise(from_unit)], _FAMILIES[family][_normalise(to_unit)]


def check_conversion(from_unit: Any, to_unit: Any) -> None:
    """Checks that a value in one unit can be converted into another. It converts nothing.

    Two units that are exactly the same text are always accepted, even if they are not convertible units (e.g., "Status" and
    "Status"): nothing needs to be converted.

    Args:
        from_unit (Any): The unit of the value.
        to_unit (Any): The unit that the value must have.

    Raises:
        ValueError: If the units are different and cannot be converted (see `_exponents`).
    """
    if _same_text(from_unit, to_unit):
        return
    _exponents(from_unit, to_unit)


def _same_text(first: Any, second: Any) -> bool:
    """Tells if two units are the same text (spaces around them are ignored, but upper and lower case count)."""
    return isinstance(first, str) and isinstance(second, str) and first.strip() == second.strip()


def build_converter(from_unit: Any, to_unit: Any) -> Callable[[Any], Any]:
    """Builds the function that converts a value from one unit to another.

    The function is built once (e.g., when an adapter starts) and used for every reading, so the units are checked
    only once. A conversion between units of the same family is done with a power of ten.

    Example:
    --------
    convert = build_converter("Wh", "kWh")
    convert(45200.0)  ->  45.2
    convert = build_converter("€/MWh", "€/kWh")
    convert(84.0)     ->  0.084
    convert = build_converter("%", "ratio")
    convert(67.0)     ->  0.67          # a percentage (0 to 100) becomes a proportion (0 to 1)
    convert = build_converter("Status", "Status")
    convert("on")     ->  "on"          # same unit, the value is returned as it is

    Args:
        from_unit (Any): The unit of the value.
        to_unit (Any): The unit that the value must have.

    Returns:
        Callable[[Any], Any]: A function that receives a value (an int or a float) in `from_unit` and returns it in
            `to_unit`. A value that is not a number is returned as it is, and a bool is not a number.

    Raises:
        ValueError: If the units are different and cannot be converted (see `_exponents`).
    """
    # Same unit: nothing to convert.
    if _same_text(from_unit, to_unit):
        return lambda value: value

    from_exponent, to_exponent = _exponents(from_unit, to_unit)
    difference = from_exponent - to_exponent

    def convert(value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return value
        if difference == 0:
            return value

        # A power of ten is applied by multiplying or dividing by an integer, which gives the number nearest to the
        # real result (multiplying by 0.001 would add tiny errors, e.g., 45200 x 0.001). The rounding removes what
        # is left of them.
        # Example: "Wh" -> "kWh": difference -3 -> 45200 / 1000 = 45.2
        converted = value * 10 ** difference if difference > 0 else value / 10 ** (-difference)
        return round(converted, 12)

    return convert


def convert(value: Any, from_unit: Any, to_unit: Any) -> Any:
    """Converts a value from one unit to another (see `build_converter`, which should be used for many values).

    Args:
        value (Any): The value, in `from_unit`.
        from_unit (Any): The unit of the value, e.g., "W".
        to_unit (Any): The unit that the value must have, e.g., "kW".

    Returns:
        Any: The value in `to_unit`. Example: convert(2500.0, "W", "kW") -> 2.5

    Raises:
        ValueError: If the units are different and cannot be converted.
    """
    return build_converter(from_unit, to_unit)(value)
