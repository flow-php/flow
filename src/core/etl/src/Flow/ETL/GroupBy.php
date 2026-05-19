<?php

declare(strict_types=1);

namespace Flow\ETL;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Stringable;

use function array_filter;
use function array_key_exists;
use function array_unique;
use function array_values;
use function count;
use function current;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function implode;
use function is_array;
use function is_scalar;
use function serialize;

final class GroupBy
{
    /**
     * @var array<AggregatingFunction>
     */
    private array $aggregations;

    /**
     * @var array<string, array{values?: array<string, mixed>, aggregators: array<AggregatingFunction>}>
     */
    private array $groupedTable;

    private ?Reference $pivot;

    /**
     * @var array<int, mixed>
     */
    private array $pivotColumns;

    /**
     * @var array<string, array<string, mixed>>
     */
    private array $pivotedTable;

    private readonly References $refs;

    public function __construct(string|Reference ...$entries)
    {
        $this->refs = References::init(...array_unique($entries));
        $this->aggregations = [];
        $this->groupedTable = [];
        $this->pivotedTable = [];
        $this->pivotColumns = [];
        $this->pivot = null;
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

        $this->aggregations = $aggregator;
    }

    public function group(Rows $rows, FlowContext $context): void
    {
        if ($this->pivot) {
            foreach ($rows as $row) {
                try {
                    $this->pivotColumns[] = $row->get($this->pivot)->value();
                } catch (InvalidArgumentException) {
                    $this->pivotColumns[] = null;
                }
            }

            $this->pivotColumns = array_values(array_filter(array_unique($this->pivotColumns))); // @phpstan-ignore argument.type

            foreach ($rows as $row) {
                $values = [];

                foreach ($this->refs as $ref) {
                    $values[$ref->name()] = $row->valueOf($ref);
                }

                $indexValue = $this->hash($values);

                $pivotValue = $row->valueOf($this->pivot);

                if (!array_key_exists($indexValue, $this->pivotedTable)) {
                    $this->pivotedTable[$indexValue] = [];
                }

                foreach ($this->refs as $ref) {
                    $this->pivotedTable[$indexValue][$ref->name()] = $row->valueOf($ref);
                }

                if ($pivotValue === null) {
                    continue;
                }

                $pivotValue = type_union(type_string(), type_integer())->assert($pivotValue);

                if (!array_key_exists($pivotValue, $this->pivotedTable[$indexValue])) {
                    /** @phpstan-ignore-next-line */
                    $this->pivotedTable[$indexValue][$pivotValue] = clone current($this->aggregations);
                }

                $aggregator = $this->pivotedTable[$indexValue][$pivotValue];

                if ($aggregator instanceof AggregatingFunction) {
                    $aggregator->aggregate($row, $context);
                }
            }
        } else {
            foreach ($rows as $row) {
                /** @var array<string, null|mixed> $values */
                $values = [];

                foreach ($this->refs as $ref) {
                    try {
                        $values[$ref->name()] = $row->get($ref)->value();
                    } catch (InvalidArgumentException) {
                        $values[$ref->name()] = null;
                    }
                }

                $valuesHash = $this->hash($values);

                if (!array_key_exists($valuesHash, $this->groupedTable)) {
                    $aggregators = [];

                    foreach ($this->aggregations as $aggregator) {
                        $aggregators[] = clone $aggregator;
                    }

                    $this->groupedTable[$valuesHash] = [
                        'values' => $values,
                        'aggregators' => $aggregators,
                    ];
                }

                foreach ($this->groupedTable[$valuesHash]['aggregators'] as $aggregator) {
                    $aggregator->aggregate($row, $context);
                }
            }
        }
    }

    public function pivot(Reference $ref): void
    {
        $this->pivot = $ref;
    }

    public function result(FlowContext $context): Rows
    {
        $rows = [];

        if ($this->pivot) {
            foreach ($this->pivotedTable as $index => $columns) {
                $row = [$this->refs->first()->name() => $index];

                foreach ($columns as $rowIndex => $values) {
                    $row[$rowIndex] = $values instanceof AggregatingFunction
                        ? $values->result($context->entryFactory())->value()
                        : $values;
                }

                foreach ($this->pivotColumns as $column) {
                    $column = type_union(type_string(), type_integer())->assert($column);

                    if (!array_key_exists($column, $row)) {
                        $row[$column] = null;
                    }
                }

                $rows[] = $row;
            }

            return array_to_rows($rows, $context->entryFactory());
        }

        foreach ($this->groupedTable as $group) {
            $entries = [];

            /** @var mixed $value */
            foreach ($group['values'] ?? [] as $entry => $value) {
                $entries[] = $context->entryFactory()->create($entry, $value);
            }

            foreach ($group['aggregators'] as $aggregator) {
                $entries[] = $aggregator->result($context->entryFactory());
            }

            if (count($entries)) {
                $rows[] = Row::create(...$entries);
            }
        }

        return new Rows(...$rows);
    }

    /**
     * @param array<array-key, mixed> $values
     */
    private function hash(array $values): string
    {
        /** @var array<string> $stringValues */
        $stringValues = [];

        /** @var mixed $value */
        foreach ($values as $value) {
            if ($value === null) {
                $stringValues[] = 'null';
            } elseif (is_scalar($value)) {
                $stringValues[] = (string) $value;
            } else {
                if ($value instanceof Stringable) {
                    $stringValues[] = $value->__toString();
                } elseif ($value instanceof DateTimeInterface) {
                    $stringValues[] = $value->format(DateTimeImmutable::ATOM);
                } elseif (is_array($value)) {
                    $stringValues[] = $this->hash($value);
                } else {
                    $stringValues[] = serialize($value);
                }
            }
        }

        return NativePHPHash::xxh128(implode('', $stringValues));
    }
}
