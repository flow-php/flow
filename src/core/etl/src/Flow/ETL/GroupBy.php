<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy\Aggregators;
use Flow\ETL\GroupBy\GroupKey;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Generator;

use function array_filter;
use function array_key_exists;
use function array_unique;
use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class GroupBy
{
    private const int RESULT_BATCH_SIZE = 1000;

    private Aggregators $aggregations;

    private ?Reference $pivot = null;

    private readonly References $refs;

    public function __construct(string|Reference ...$entries)
    {
        $this->refs = References::init(...array_unique($entries));
        $this->aggregations = new Aggregators();
    }

    public function aggregate(AggregatingFunction ...$aggregator): void
    {
        if (!count($aggregator)) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        if ($this->pivot !== null && count($aggregator) !== 1) {
            throw new RuntimeException(
                'Pivot requires exactly one aggregation in group by, given: ' . count($aggregator),
            );
        }

        $this->aggregations = new Aggregators(...$aggregator);
    }

    public function aggregatedRow(GroupKey $key, Aggregators $aggregators, EntryFactory $entryFactory): Row
    {
        $entries = [];

        /** @var mixed $value */
        foreach ($key as $name => $value) {
            $entries[] = $entryFactory->create($name, $value);
        }

        foreach ($aggregators as $aggregator) {
            $entries[] = $aggregator->result($entryFactory);
        }

        return Row::create(...$entries);
    }

    public function aggregations(): Aggregators
    {
        return $this->aggregations;
    }

    public function isPivot(): bool
    {
        return $this->pivot !== null;
    }

    public function keyValues(Row $row): GroupKey
    {
        $values = [];

        foreach ($this->refs as $ref) {
            try {
                $values[$ref->name()] = $row->valueOf($ref);
            } catch (InvalidArgumentException) {
                $values[$ref->name()] = null;
            }
        }

        return new GroupKey($values);
    }

    public function pivot(Reference $ref): void
    {
        $this->pivot = $ref;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function pivotResult(Generator $rows, FlowContext $context): Generator
    {
        $pivot = $this->pivot;

        if ($pivot === null) {
            throw new RuntimeException('pivotResult() called without a pivot reference');
        }

        if ($this->aggregations->count() === 0) {
            throw new RuntimeException('Pivot requires exactly one aggregation');
        }

        $aggregation = $this->aggregations->first();

        /** @var array<int, null|array<array-key, mixed>|bool|float|int|object|string> $pivotColumns */
        $pivotColumns = [];
        /** @var array<string, array<string, AggregatingFunction|null|array<array-key, mixed>|bool|float|int|object|string>> $pivotedTable */
        $pivotedTable = [];

        foreach ($rows as $batch) {
            foreach ($batch as $row) {
                try {
                    $pivotColumns[] = $row->valueOf($pivot);
                } catch (InvalidArgumentException) {
                    $pivotColumns[] = null;
                }
            }

            foreach ($batch as $row) {
                $values = [];

                foreach ($this->refs as $ref) {
                    $values[$ref->name()] = $row->valueOf($ref);
                }

                $indexValue = (string) new GroupKey($values);
                $pivotValue = $row->valueOf($pivot);

                if (!array_key_exists($indexValue, $pivotedTable)) {
                    $pivotedTable[$indexValue] = [];
                }

                foreach ($this->refs as $ref) {
                    $pivotedTable[$indexValue][$ref->name()] = $row->valueOf($ref);
                }

                if ($pivotValue === null) {
                    continue;
                }

                $pivotValue = type_union(type_string(), type_integer())->assert($pivotValue);

                if (!array_key_exists($pivotValue, $pivotedTable[$indexValue])) {
                    $pivotedTable[$indexValue][$pivotValue] = clone $aggregation;
                }

                $aggregator = $pivotedTable[$indexValue][$pivotValue];

                if ($aggregator instanceof AggregatingFunction) {
                    $aggregator->aggregate($row, $context);
                }
            }
        }

        $pivotColumns = array_values(array_filter(array_unique($pivotColumns)));

        $buffer = [];

        foreach ($pivotedTable as $index => $columns) {
            $row = [$this->refs->first()->name() => $index];

            foreach ($columns as $rowIndex => $value) {
                $row[$rowIndex] = $value instanceof AggregatingFunction
                    ? $value->result($context->entryFactory())->value()
                    : $value;
            }

            foreach ($pivotColumns as $column) {
                $column = type_union(type_string(), type_integer())->assert($column);

                if (!array_key_exists($column, $row)) {
                    $row[$column] = null;
                }
            }

            $buffer[] = $row;

            if (count($buffer) >= self::RESULT_BATCH_SIZE) {
                yield array_to_rows($buffer, $context->entryFactory());
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield array_to_rows($buffer, $context->entryFactory());
        }
    }
}
