# Algorithm message format

`algorithm_message_format.json` is an example of the message of one period, sent to EnergAIze.

## Structure

```
{
  "period_start": ...,     The start of the period
  "period_end": ...,       The end of the period
  "entities": {...},       The data of the entities of the environment
  "derived_data": {...},   Data calculated from the entities
  "community": {...}       Data of the community as a whole (only when the installation is part of a REC)
}
```

### Period of the message and of the values

Every message has a `period_start` and a `period_end`: the start and the end of the period it refers to. The start is
`period_end - frequency`. The message is published after the grace period, so late readings can still enter it.

These two terms also appear in the lists of values of the message (`original_values`, `final_values` and `values`).
There, each item has its own `period_start` and `period_end`, and its value refers to the period between them:

```json
{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000", "value": 0.00874}
```

### `entities`

Each entity has its `entity_type_id` and its `parameters`.

```json
"entities": {
  "GR01": {
    "entity_type_id": "grid_meter",
    "parameters": {
      "gr01_energy_in_total": {
        "parameter_type_id": "energy_in_total",
        "original_values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
                             "value": 0.00437},
                            {"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
                             "value": 0.00437000000025}],
        "final_values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
                          "value": 0.00874000000025}],
        "measurement_unit": "kWh",
        "aggregation_method": "sum",
        "generated": false
      }
    }
  },
  "EM01": {
    "entity_type_id": "energy_market",
    "parameters": {
      "em01_energy_price": {
        "parameter_type_id": "energy_price",
        "original_values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:34:15 +0000",
                             "value": 0.20789},
                            {"period_start": "2026-11-09 16:34:15 +0000", "period_end": "2026-11-09 16:49:15 +0000",
                             "value": 0.2318}, ...],
        "final_values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:34:15 +0000",
                          "value": 0.20789},
                         {"period_start": "2026-11-09 16:34:15 +0000", "period_end": "2026-11-09 16:49:15 +0000",
                          "value": 0.2318}, ...],
        "measurement_unit": "€/kWh",
        "aggregation_method": null,
        "generated": false
      }
    }
  }
}
```

Each parameter has:

| Field | Meaning                                                                                                              |
|---|----------------------------------------------------------------------------------------------------------------------|
| `parameter_type_id` | Type of the parameter (e.g., `energy_in_total`).                                                                     |
| `original_values` | The values after the Data Issue Handling approaches (e.g., missing data filled in) and before the Combination Logic. |
| `final_values` | The values after the Combination Logic.                                                                              |
| `measurement_unit` | Measurement unit of the values.                                                                                      |
| `aggregation_method` | How the Combination Logic combines the readings of the period into one value (e.g., `sum`, `average`, `last_value`). |
| `generated` | `true` if the values were not measured in the period (e.g., filled in).                                              |

When an entity sent no data and nothing could be filled in, `original_values` and `final_values` are `null`. `null`
means "no data", and it is not the same as `0.0`, which is a real value.

### `derived_data`

Data calculated from the `final_values` of the parameters. 

```json
"derived_data": {
  "solar_generation": {
    "values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
               "value": 0.0}],
    "measurement_unit": "kWh",
    "aggregation_method": "sum",
    "generated": false
  },
  "non_shiftable_load": {
    "values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
               "value": 0.00874000000025}],
    "measurement_unit": "kWh",
    "aggregation_method": "sum",
    "generated": false
  }
}
```

| Name | Formula |
|---|---|
| `solar_generation` | Sum of the energy of all the PV panels. |
| `non_shiftable_load` | `grid_in + solar_generation - grid_out + battery_discharge - battery_charge` (never below 0). |

Each one has:

| Field | Meaning                                                                                                                                       |
|---|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `values` | A list with the result for the period                                                                                                         |
| `measurement_unit` | Measurement unit of the values.                                                                                                               |
| `aggregation_method` | How the values were combined (e.g., `sum`).                                                                                                   |
| `generated` | `true` if the result is not real, e.g., one of the data needed did not arrive, is malformed or is in a measurement unit that is not suitable. |

If any of the problems above (or any other) happens to a datum, the calculation is made without it and `generated` is
`true`. If there is no value at all for the calculation of a derived datum, its `values` is `null` (and `generated` is
`true`). `null` means "no data", and it is not the same as `0.0`, which is a real value.

More derived data may be added in the future.

### `community`

Only present when the installation is part of a REC. It has the data of the community as a whole.

```json
"community": {
  "energy_in_total": {
    "values": [{"period_start": "2026-11-09 16:19:15 +0000", "period_end": "2026-11-09 16:19:30 +0000",
               "value": 0.01022000000025}],
    "generated": false,
    "measurement_unit": "kWh"
  },
  "energy_out_total": {...}
}
```

| Name | Meaning |
|---|---|
| `energy_in_total` | Energy taken from the grid by the installations of the community that shared their data for the same period, plus the installation itself. |
| `energy_out_total` | Energy given to the grid by the same installations. |

Each one has:

| Field | Meaning |
|---|---|
| `values` | A list with the result for the period. |
| `measurement_unit` | Measurement unit of the values. |
| `generated` | `true` if the result is not real, e.g., one of the data needed did not arrive, is malformed or is in a measurement unit that is not suitable. |

If any of the problems above (or any other) happens to a datum, the calculation is made without it and `generated` is
`true`. If there is no value at all for the calculation of a derived datum, its `values` is `null` (and `generated` is
`true`). `null` means "no data", and it is not the same as `0.0`, which is a real value.

More fields may be added to `community` in the future.
