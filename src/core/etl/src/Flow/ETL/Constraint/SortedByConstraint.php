<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use DateInterval;
use DateTimeInterface;
use Flow\ETL\Constraint;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;

use function implode;
use function is_array;
use function is_numeric;
use function is_string;
use function var_export;

final class SortedByConstraint implements Constraint
{
    private bool $firstRow = true;

    /**
     * @var array<string, null|array<array-key, mixed>|bool|float|int|object|string>
     */
    private array $previousValues = [];

    private readonly References $references;

    public function __construct(Reference $column, Reference ...$columns)
    {
        $this->references = new References($column, ...$columns);
    }

    public function isSatisfiedBy(Row $row): bool
    {
        if ($this->firstRow) {
            foreach ($this->references->all() as $reference) {
                $this->previousValues[$reference->name()] = $row->valueOf($reference);
            }
            $this->firstRow = false;

            return true;
        }

        foreach ($this->references->all() as $reference) {
            $currentValue = $row->valueOf($reference);
            $previousValue = $this->previousValues[$reference->name()];

            $direction = $reference->sort();

            if ($previousValue === null && $currentValue === null) {
                $comparison = 0;
            } elseif ($previousValue === null) {
                $comparison = $direction === SortOrder::ASC ? -1 : 1;
            } elseif ($currentValue === null) {
                $comparison = $direction === SortOrder::ASC ? 1 : -1;
            } elseif (is_numeric($previousValue) && is_numeric($currentValue)) {
                $prev = (float) $previousValue;
                $curr = (float) $currentValue;
                $comparison = $direction === SortOrder::ASC ? $prev <=> $curr : $curr <=> $prev;
            } elseif (is_string($previousValue) && is_string($currentValue)) {
                $comparison = $direction === SortOrder::ASC
                    ? $previousValue <=> $currentValue
                    : $currentValue <=> $previousValue;
            } elseif ($previousValue instanceof DateTimeInterface && $currentValue instanceof DateTimeInterface) {
                $comparison = $direction === SortOrder::ASC
                    ? $previousValue <=> $currentValue
                    : $currentValue <=> $previousValue;
            } elseif ($previousValue instanceof DateInterval && $currentValue instanceof DateInterval) {
                $comparison = $direction === SortOrder::ASC
                    ? $previousValue <=> $currentValue
                    : $currentValue <=> $previousValue;
            } elseif (is_array($previousValue) && is_array($currentValue)) {
                $comparison = $direction === SortOrder::ASC
                    ? $previousValue <=> $currentValue
                    : $currentValue <=> $previousValue;
            } else {
                $comparison = 0;
            }

            if ($comparison < 0) {
                foreach ($this->references->all() as $ref) {
                    $this->previousValues[$ref->name()] = $row->valueOf($ref);
                }

                return true;
            }

            if ($comparison > 0) {
                return false;
            }
        }

        foreach ($this->references->all() as $reference) {
            $this->previousValues[$reference->name()] = $row->valueOf($reference);
        }

        return true;
    }

    public function toString(): string
    {
        $columns = [];

        foreach ($this->references->all() as $reference) {
            $columns[] = $reference->name() . ' ' . $reference->sort()->name;
        }

        return sprintf('Sorted constraint on [%s]', implode(', ', $columns));
    }

    public function violation(Row $row): string
    {
        $violations = [];

        foreach ($this->references->all() as $reference) {
            $entry = $row->get($reference);
            $previousValue = $this->previousValues[$reference->name()] ?? null;

            $violations[] = sprintf(
                '%s<%s> expected %s order, current: %s, previous: %s',
                $entry->name(),
                $entry->type()->toString(),
                $reference->sort()->name,
                $entry->toString(),
                $previousValue === null ? 'null' : var_export($previousValue, true),
            );
        }

        return implode('; ', $violations);
    }
}
