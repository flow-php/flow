<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use Flow\ETL\Constraint;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;

use function implode;
use function var_export;

final class SortedByConstraint implements Constraint
{
    private bool $firstRow = true;

    /**
     * @var array<string, mixed>
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

            $comparison = match ($reference->sort()) {
                SortOrder::ASC => $previousValue <=> $currentValue,
                SortOrder::DESC => $currentValue <=> $previousValue,
            };

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
