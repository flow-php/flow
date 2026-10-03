# Aggregations

[DOC_LINK:/documentation/components/core/group-by.md]

[TOC]

Each group created by `groupBy` function can be aggregated using one of the aggregation functions.

## Aggregation functions

- [`COUNT()`](/src/core/etl/src/Flow/ETL/Function/Count.php)
- [`AVERAGE()`](/src/core/etl/src/Flow/ETL/Function/Average.php)
- [`SUM()`](/src/core/etl/src/Flow/ETL/Function/Sum.php)
- [`MIN()`](/src/core/etl/src/Flow/ETL/Function/Min.php)
- [`MAX()`](/src/core/etl/src/Flow/ETL/Function/Max.php)
- [`COLLECT()`](/src/core/etl/src/Flow/ETL/Function/Collect.php)
- [`COLLECT_UNIQUE()`](/src/core/etl/src/Flow/ETL/Function/CollectUnique.php)
- [`FIRST()`](/src/core/etl/src/Flow/ETL/Function/First.php)
- [`LAST()`](/src/core/etl/src/Flow/ETL/Function/Last.php)
- [`STRING_AGG()`](/src/core/etl/src/Flow/ETL/Function/StringAggregate.php)

All aggregation functions implement [`AggregatingFunction`](/src/core/etl/src/Flow/ETL/Function/AggregatingFunction.php):
`aggregate(Rows $rows, array $indices, FlowContext $context)` folds the rows of one group (`$indices` of the batch)
into the aggregator's state, and `merge()` folds in the partial state of another aggregator of the same class, which
group by uses after it spilled.

## Float arithmetic

`sum()` and `average()` add floats as IEEE 754 doubles with compensated summation. `exact: true` switches to decimal
arithmetic:

```php
<?php

use function Flow\ETL\DSL\{df, from_array, ref, sum};

df()->read(from_array([['a' => 0.1], ['a' => 0.2]]))->aggregate([sum(ref('a'))]);              // 0.30000000000000004
df()->read(from_array([['a' => 0.1], ['a' => 0.2]]))->aggregate([sum(ref('a'), exact: true)]); // 0.3
```

## Example

```php
<?php

use Flow\Calculator\Rounding;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{average, concat, data_frame, lit, max, min, ref, sum, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/power-plant-daily.csv', separator: ';'))
    ->withEntry('production_kwh', ref('Produkcja(kWh)'))
    ->withEntry('consumption_kwh', ref('Zużycie(kWh)'))
    ->withEntry('date', ref('Zaktualizowany czas')->toDate('Y/m/d')->dateFormat('Y/m'))
    ->select('date', 'production_kwh', 'consumption_kwh')
    ->groupBy([ref('date')])
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
    ->withEntry('consumption', ref('consumption_kwh_sum')->divide(ref('production_kwh_sum'), 4, Rounding::HALF_UP))
    ->withEntry('consumption', ref('consumption')->multiply(lit(100))->round(lit(2)))
    ->withEntry('consumption', concat(ref('consumption'), lit('%')))
    ->write(to_output(truncate: false))
    ->run();
```