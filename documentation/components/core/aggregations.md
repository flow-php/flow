# Aggregations

[DOC_LINK:/documentation/components/core/group-by.md]

[TOC]

Each group created by `groupBy` function can be aggregated using one of the aggregation functions.

## Aggregation functions

- [`COUNT()`](/src/core/etl/src/Flow/ETL/Functions/Count.php)
- [`AVERAGE()`](/src/core/etl/src/Flow/ETL/Functions/Average.php)
- [`SUM()`](/src/core/etl/src/Flow/ETL/Functions/Sum.php)
- [`MIN()`](/src/core/etl/src/Flow/ETL/Functions/Min.php)
- [`MAX()`](/src/core/etl/src/Flow/ETL/Functions/Max.php)
- [`COLLECT()`](/src/core/etl/src/Flow/ETL/Functions/Collect.php)
- [`COLLECT_UNIQUE()`](/src/core/etl/src/Flow/ETL/Functions/CollectUnique.php)
- [`FIRST()`](/src/core/etl/src/Flow/ETL/Functions/First.php)
- [`LAST()`](/src/core/etl/src/Flow/ETL/Functions/Last.php)
- [`STRING_AGG()`](/src/core/etl/src/Flow/ETL/Functions/StringAggregate.php)

All aggregation functions implement [`AggregatingFunction`](/src/core/etl/src/Flow/ETL/Functions/AggregatingFunction.php).
`aggregate(Rows $rows, array $indices, Backend $backend)` folds the rows of one group into the state.
`merge(array $partial, Backend $backend)` folds in the `partial()` of another aggregator of the same function.
Group by merges after it spills.

## Custom aggregators

A custom aggregator also describes itself (it is rebuilt in a worker from `describe()`) and exposes its state as a
partial, the plain values a worker hands back for the parent or a later task to merge:

```php
<?php

final class LongestString implements AggregatingFunction
{
    private ?string $longest = null;

    public function __construct(private readonly Reference $ref) {}

    public static function fromDescription(array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['ref'], Reference::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['ref' => $this->ref]);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $column = $rows->column($this->ref->to());

        foreach ($indices as $index) {
            $value = $column->value($index);

            if (is_string($value) && strlen($value) > strlen($this->longest ?? '')) {
                $this->longest = $value;
            }
        }
    }

    public function partialType(): StructureType
    {
        return type_structure(['longest' => type_optional(type_string())]);
    }

    public function partial(): array
    {
        return ['longest' => $this->longest];
    }

    public function merge(array $partial, Backend $backend): void
    {
        $longest = $this->partialType()->assert($partial)['longest'];

        if ($longest !== null && strlen($longest) > strlen($this->longest ?? '')) {
            $this->longest = $longest;
        }
    }

    public function withChildren(array $children): static
    {
        return new self($children[0]);   // a fresh instance: no state crosses
    }

    // children(), resolved(), deterministic(), references(), returns(), outputName(), value() as before
}
```

`partialType()` is valid once the aggregator is resolved; `partial()` of a fresh instance is the initial state, and
merging it is a no-op. Partials of two tasks merge in task order: an order-sensitive aggregator (`first()`,
`last()`, `collect()`) relies on it. A `describe()` returning `null` keeps the group by in the parent process.

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

use Flow\ETL\Foundation\Calculator\Rounding;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{average, concat, data_frame, lit, max, min, ref, sum, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/power-plant-daily.csv', separator: ';'))
    ->withColumn('production_kwh', ref('Produkcja(kWh)'))
    ->withColumn('consumption_kwh', ref('Zużycie(kWh)'))
    ->withColumn('date', ref('Zaktualizowany czas')->toDate('Y/m/d')->dateFormat('Y/m'))
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
    ->withColumn('production_kwh_avg', ref('production_kwh_avg')->round(lit(2)))
    ->withColumn('consumption_kwh_avg', ref('consumption_kwh_avg')->round(lit(2)))
    ->withColumn('production_kwh_min', ref('production_kwh_min')->round(lit(2)))
    ->withColumn('consumption_kwh_min', ref('consumption_kwh_min')->round(lit(2)))
    ->withColumn('production_kwh_max', ref('production_kwh_max')->round(lit(2)))
    ->withColumn('consumption_kwh_max', ref('consumption_kwh_max')->round(lit(2)))
    ->withColumn('production_kwh_sum', ref('production_kwh_sum')->round(lit(2)))
    ->withColumn('consumption_kwh_sum', ref('consumption_kwh_sum')->round(lit(2)))
    ->withColumn('consumption', ref('consumption_kwh_sum')->divide(ref('production_kwh_sum'), 4, Rounding::HALF_UP))
    ->withColumn('consumption', ref('consumption')->multiply(lit(100))->round(lit(2)))
    ->withColumn('consumption', concat(ref('consumption'), lit('%')))
    ->write(to_output(truncate: false))
    ->run();
```