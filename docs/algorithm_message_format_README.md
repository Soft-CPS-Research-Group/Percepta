# Algorithm message format

`algorithm_message_format.json` is an example of the message of one period, as it leaves Data Processing (and the
REC Bridge, if the instance uses it) towards Inference and the ML model. It comes from a real message of R-H-01.

## Structure

```
{
  "period_start": ...,     The start of the period
  "period_end": ...,       The end of the period
  "entities": {...},       The data of the entities of the environment
  "derived_data": {...},   Data worked out from the entities
  "community": {...}       Totals of the REC (only with the REC Bridge)
}
```

### `period_start` and `period_end`

The start and the end of the period of the message. The start is `period_end - frequency`, and it is the `start_date`
of the data of the period. The message is published after the grace period, so late readings can still enter it.

`period_end` also identifies the period: the REC Bridge only combines messages of the installations that have the same
`period_end`.

### `entities`

Each entity has its `entity_type_id` and its `parameters`. Each parameter has:

| Field | Meaning |
|---|---|
| `parameter_type_id` | Type of the parameter (e.g., `energy_in_total`). |
| `original_values` | The values as they arrived, after the Combination Logic. |
| `final_values` | The values after the Data Issue Handling approaches (e.g., missing data filled in). |
| `measurement_unit` | Unit of the values. |
| `aggregation_method` | How readings of the period are combined (e.g., `sum`, `average`, `last_value`). |
| `start_date` | Start of the period. |
| `frequency_seconds` | Length of the period. |
| `generated` | `true` if the values were not measured in the period (e.g., filled in). |

When an entity sent no data and nothing could be filled in, `original_values` and `final_values` are `null`. `null`
means "no data", and it is not the same as `0.0`, which is a real value.

### `derived_data`

Data worked out from the `final_values` of the entities. Each one has a single list `values`, plus
`measurement_unit`, `aggregation_method`, `start_date`, `frequency_seconds` and `generated`.

| Name | Formula |
|---|---|
| `solar_generation` | Sum of the energy of all the PV panels. |
| `non_shiftable_load` | `grid_in + solar_generation - grid_out + battery_discharge - battery_charge` (never below 0). |

The sums are in **kWh**. A value in another unit (e.g., Wh) is converted before adding. A value whose unit cannot be
converted is not used, and the result is `generated: true`. A parameter with no value also makes the result
`generated`.

### `community`

Only present when the instance uses the REC Bridge. It has the totals of the installations of the Renewable Energy
Community that shared their data for the same period, plus the installation itself.

```json
"community": {
  "energy_in_total": {
    "value": 0.01022000000025,
    "generated": false,
    "start_date": "2026-11-09 16:19:15 +0000",
    "measurement_unit": "kWh"
  },
  "energy_out_total": {...}
}
```

| Field | Meaning |
|---|---|
| `value` | Sum of the values that exist, or `null` if no installation has one. |
| `generated` | `true` if the sum is not complete: a value is `null`, an installation has no parameter for the total, a value could not be used, or a peer did not send its data in time. |
| `start_date` | Start of the period. |
| `measurement_unit` | Always `kWh`. Values in another unit are converted before adding. |

Each total has its own `generated`: a `null` in `energy_in_total` does not mark `energy_out_total`.

Only the parameters marked with `"rec_relevant": true` in the environment are shared with the other installations.
