<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Factory\NativeEntryFactory;

final class GroupBy
{
    /**
     * @var array<Aggregation>
     */
    private array $aggregations;

    /**
     * @var array<string>
     */
    private array $entries;

    /**
     * @var array<string, array{values?: array<string, mixed>, aggregators: array<Aggregator>}>
     */
    private array $groups;

    public function __construct(string ...$entries)
    {
        $this->entries = $entries;
        $this->aggregations = [];
        $this->groups = [];
    }

    public function aggregate(Aggregation ...$aggregations) : void
    {
        if (!\count($aggregations)) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        $this->aggregations = $aggregations;
    }

    public function group(Rows $rows) : void
    {
        foreach ($rows as $row) {
            /** @var array<string, null|mixed> $values */
            $values = [];

            foreach ($this->entries as $entryName) {
                try {
                    $value = $row->valueOf($entryName);

                    if (!\is_scalar($value) && null !== $value) {
                        throw new RuntimeException('Grouping by non scalar values is not supported, given: ' . \gettype($value));
                    }

                    /** @psalm-suppress MixedAssignment */
                    $values[$entryName] = $value;
                } catch (InvalidArgumentException $e) {
                    $values[$entryName] = null;
                }
            }

            $valuesHash = $this->hash($values);

            if (!\array_key_exists($valuesHash, $this->groups)) {
                $this->groups[$valuesHash] = [
                    'values' => $values,
                    'aggregators' => [],
                ];

                foreach ($this->aggregations as $aggregation) {
                    $this->groups[$valuesHash]['aggregators'][] = $aggregation->create();
                }
            }

            foreach ($this->groups[$valuesHash]['aggregators'] as $aggregator) {
                $aggregator->aggregate($row);
            }
        }
    }

    public function result() : Rows
    {
        $rows = new Rows();

        foreach ($this->groups as $group) {
            $entries = new Entries();

            if (\array_key_exists('values', $group)) {
                /** @var mixed $value */
                foreach ($group['values'] as $entry => $value) {
                    $entries = $entries->add((new NativeEntryFactory)->create($entry, $value));
                }
            }

            foreach ($group['aggregators'] as $aggregator) {
                $entries = $entries->add($aggregator->result());
            }

            if (\count($entries)) {
                $rows = $rows->add(new Row($entries));
            }
        }

        return $rows;
    }

    /**
     * @param array<array-key, mixed> $values
     *
     * @return string
     */
    private function hash(array $values) : string
    {
        /** @var array<string> $stringValues */
        $stringValues = [];

        /** @var mixed $value */
        foreach ($values as $value) {
            if ($value === null) {
                $stringValues[] =\hash('sha256', 'null');
            } elseif (\is_scalar($value)) {
                $stringValues[] = (string) $value;
            }
        }

        return \hash('sha256', \implode('', $stringValues));
    }
}
