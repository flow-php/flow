# Aggregations

- [⬅️️ Back](group-by.md)

Each group created by `groupBy` function can be aggregated using one of the aggregation functions.

## Aggregation functions

- [`COUNT()`](../../../src/core/etl/src/Flow/ETL/Function/Count.php)
- [`AVERAGE()`](../../../src/core/etl/src/Flow/ETL/Function/Average.php)
- [`SUM()`](../../../src/core/etl/src/Flow/ETL/Function/Sum.php)
- [`MIN()`](../../../src/core/etl/src/Flow/ETL/Function/Min.php)
- [`MAX()`](../../../src/core/etl/src/Flow/ETL/Function/Max.php)
- [`COLLECT()`](../../../src/core/etl/src/Flow/ETL/Function/Collect.php)
- [`COLLECT_UNIQUE()`](../../../src/core/etl/src/Flow/ETL/Function/CollectUnique.php)
- [`FIRST`](../../../src/core/etl/src/Flow/ETL/Function/First.php)
- [`LAST`](../../../src/core/etl/src/Flow/ETL/Function/Last.php)

All aggregation functions are implementing [`AggregatingFunction`](../../../src/core/etl/src/Flow/ETL/Function/AggregatingFunction.php) interface.

## Example

```php
<?php 

data_frame()
    ->read(from_csv(__DIR__ . '/power-plant-daily.csv', delimiter: ';'))
    ->withEntry('production_kwh', ref('Produkcja(kWh)'))
    ->withEntry('consumption_kwh', ref('Zużycie(kWh)'))
    ->withEntry('date', ref('Zaktualizowany czas')->toDate('Y/m/d')->dateFormat('Y/m'))
    ->select('date', 'production_kwh', 'consumption_kwh')
    ->groupBy(ref('date'))
    ->aggregate(
        average(ref('production_kwh')),
        average(ref('consumption_kwh')),
        min(ref('production_kwh')),
        min(ref('consumption_kwh')),
        max(ref('production_kwh')),
        max(ref('consumption_kwh')),
        sum(ref('production_kwh')),
        sum(ref('consumption_kwh'))
    )
    ->withEntry('production_kwh_avg', ref('production_kwh_avg')->round(lit(2)))
    ->withEntry('consumption_kwh_avg', ref('consumption_kwh_avg')->round(lit(2)))
    ->withEntry('production_kwh_min', ref('production_kwh_min')->round(lit(2)))
    ->withEntry('consumption_kwh_min', ref('consumption_kwh_min')->round(lit(2)))
    ->withEntry('production_kwh_max', ref('production_kwh_max')->round(lit(2)))
    ->withEntry('consumption_kwh_max', ref('consumption_kwh_max')->round(lit(2)))
    ->withEntry('production_kwh_sum', ref('production_kwh_sum')->round(lit(2)))
    ->withEntry('consumption_kwh_sum', ref('consumption_kwh_sum')->round(lit(2)))
    ->withEntry('consumption', ref('consumption_kwh_sum')->divide(ref('production_kwh_sum')))
    ->withEntry('consumption', ref('consumption')->multiply(lit(100))->round(lit(2)))
    ->withEntry('consumption', concat(ref('consumption'), lit('%')))
    ->write(to_output(truncate: false))
    ->run();
```