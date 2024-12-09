<?php

declare(strict_types=1);

namespace Flow\ETL;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\{Reference, References};

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

    private array $pivotColumns;

    private array $pivotedTable;

    private readonly References $refs;

    public function __construct(string|Reference ...$entries)
    {
        $this->refs = References::init(...\array_unique($entries));
        $this->aggregations = [];
        $this->groupedTable = [];
        $this->pivotedTable = [];
        $this->pivotColumns = [];
        $this->pivot = null;
    }

    public function aggregate(AggregatingFunction ...$aggregator) : void
    {
        if (!\count($aggregator)) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        if ($this->pivot !== null && \count($aggregator) !== 1) {
            throw new RuntimeException('Pivot requires exactly one aggregation in group by, given: ' . \count($aggregator));
        }

        $this->aggregations = $aggregator;
    }

    public function group(Rows $rows) : void
    {
        if ($this->pivot) {
            foreach ($rows as $row) {
                try {
                    $this->pivotColumns[] = $row->get($this->pivot)->value();
                } catch (InvalidArgumentException) {
                    $this->pivotColumns[] = null;
                }
            }

            $this->pivotColumns = \array_values(\array_filter(\array_unique($this->pivotColumns)));

            $indexRef = $this->refs->first();

            foreach ($rows as $row) {
                $indexValue = $row->valueOf($indexRef);
                $pivotValue = $row->valueOf($this->pivot);

                if (!\array_key_exists($indexValue, $this->pivotedTable)) {
                    $this->pivotedTable[$indexValue] = [];
                }

                if ($pivotValue === null) {
                    continue;
                }

                if (!\array_key_exists($pivotValue, $this->pivotedTable[$indexValue])) {
                    /** @phpstan-ignore-next-line */
                    $this->pivotedTable[$indexValue][$pivotValue] = clone \current($this->aggregations);
                }

                $this->pivotedTable[$indexValue][$pivotValue]->aggregate($row);
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

                if (!\array_key_exists($valuesHash, $this->groupedTable)) {
                    $this->groupedTable[$valuesHash] = [
                        'values' => $values,
                        'aggregators' => [],
                    ];

                    foreach ($this->aggregations as $aggregator) {
                        $this->groupedTable[$valuesHash]['aggregators'][] = clone $aggregator;
                    }
                }

                foreach ($this->groupedTable[$valuesHash]['aggregators'] as $aggregator) {
                    $aggregator->aggregate($row);
                }
            }
        }
    }

    public function pivot(Reference $ref) : void
    {
        if ($this->refs->count() !== 1) {
            throw new RuntimeException('Pivot requires exactly one entry reference in group by, given: ' . $this->refs->count() . '');
        }

        $this->pivot = $ref;
    }

    public function result(FlowContext $context) : Rows
    {
        $rows = [];

        if ($this->pivot) {
            foreach ($this->pivotedTable as $index => $columns) {
                $row = [$this->refs->first()->name() => $index];

                foreach ($columns as $rowIndex => $values) {
                    $row[$rowIndex] = $values->result()->value();
                }

                foreach ($this->pivotColumns as $column) {
                    if (!\array_key_exists($column, $row)) {
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
                $entries[] = $aggregator->result();
            }

            if (\count($entries)) {
                $rows[] = Row::create(...$entries);
            }
        }

        return new Rows(...$rows);
    }

    /**
     * @param array<array-key, mixed> $values
     */
    private function hash(array $values) : string
    {
        /** @var array<string> $stringValues */
        $stringValues = [];

        /** @var mixed $value */
        foreach ($values as $value) {
            if ($value === null) {
                $stringValues[] = 'null';
            } elseif (\is_scalar($value)) {
                $stringValues[] = (string) $value;
            } else {
                if ($value instanceof \Stringable) {
                    $stringValues[] = $value->__toString();
                } elseif ($value instanceof \DateTimeInterface) {
                    $stringValues[] = $value->format(\DateTimeImmutable::ATOM);
                } elseif (\is_array($value)) {
                    $stringValues[] = $this->hash($value);
                } else {
                    $stringValues[] = \serialize($value);
                }
            }
        }

        return NativePHPHash::xxh128(\implode('', $stringValues));
    }
}
