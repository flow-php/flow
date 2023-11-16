<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

final class GroupBy
{
    /**
     * @var array<AggregatingFunction>
     */
    private array $aggregations;

    /**
     * @var array<string, array{values?: array<string, mixed>, aggregators: array<AggregatingFunction>}>
     */
    private array $groups;

    private readonly References $refs;

    public function __construct(string|Reference ...$entries)
    {
        $this->refs = References::init(...\array_unique($entries));
        $this->aggregations = [];
        $this->groups = [];
    }

    public function aggregate(AggregatingFunction ...$aggregator) : void
    {
        if (!\count($aggregator)) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        $this->aggregations = $aggregator;
    }

    public function group(Rows $rows) : void
    {
        foreach ($rows as $row) {
            /** @var array<string, null|mixed> $values */
            $values = [];

            foreach ($this->refs as $ref) {
                try {
                    $value = $row->valueOf($ref);

                    if (!\is_scalar($value) && null !== $value) {
                        throw new RuntimeException('Grouping by non scalar values is not supported, given: ' . \gettype($value));
                    }

                    $values[$ref->name()] = $value;
                } catch (InvalidArgumentException) {
                    $values[$ref->name()] = null;
                }
            }

            $valuesHash = $this->hash($values);

            if (!\array_key_exists($valuesHash, $this->groups)) {
                $this->groups[$valuesHash] = [
                    'values' => $values,
                    'aggregators' => [],
                ];

                foreach ($this->aggregations as $aggregator) {
                    $this->groups[$valuesHash]['aggregators'][] = clone $aggregator;
                }
            }

            foreach ($this->groups[$valuesHash]['aggregators'] as $aggregator) {
                $aggregator->aggregate($row);
            }
        }
    }

    public function result(FlowContext $context) : Rows
    {
        $rows = [];

        foreach ($this->groups as $group) {
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
                $stringValues[] = \hash('xxh128', 'null');
            } elseif (\is_scalar($value)) {
                $stringValues[] = (string) $value;
            }
        }

        return \hash('xxh128', \implode('', $stringValues));
    }
}
