<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeNarrower;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function is_string;
use function max;
use function trim;

final class ColumnTypes
{
    /**
     * First-seen order: header names first, then names met only in rows. Keyed by array-key, not string:
     * PHP re-keys a numeric column name to int on the way into the array, so the name is stringified again
     * in schema(), the one place it leaves this class.
     *
     * @var array<array-key, Type<mixed>>
     */
    private array $types = [];

    private int $rows = 0;

    /**
     * @param list<string> $names - columns known before any row - a header; [] for a headerless source
     */
    public function __construct(
        array $names,
        private readonly TypeNarrower $typer,
        private readonly TypeWidener $widener = new TypeWidener(),
    ) {
        foreach ($names as $name) {
            $this->types[$name] = type_null();
        }
    }

    /**
     * A fold computed elsewhere (a native reader, a subprocess) - equal to the observe() fold over the same rows.
     *
     * @param array<array-key, Type<mixed>> $types - first-seen order, header names first
     */
    public static function fromColumnTypes(
        array $types,
        int $rows,
        TypeNarrower $typer,
        TypeWidener $widener = new TypeWidener(),
    ): self {
        $columns = new self([], $typer, $widener);
        $columns->types = $types;
        $columns->rows = $rows;

        return $columns;
    }

    /**
     * @param list<string> $names - columns known before any row; [] when the format carries none (JSON)
     * @param iterable<int, array<array-key, mixed>> $rows - one unit's rows, unstarted
     * @param int<0, max>|-1 $rowBudget - rows to observe, 0 for none, -1 for all of them
     */
    public static function sniff(array $names, iterable $rows, int $rowBudget, TypeNarrower $typer): self
    {
        $columns = new self($names, $typer);

        if ($rowBudget === 0) {
            return $columns;
        }

        foreach ($rows as $row) {
            $columns->observe($row);

            if ($rowBudget !== -1 && $columns->rows() >= $rowBudget) {
                break;
            }
        }

        return $columns;
    }

    /**
     * The fold so far, closed: what crosses a process boundary and what the inferrer merges.
     */
    public function folded(): SniffedColumns
    {
        return new SniffedColumns($this->types, max(0, $this->rows), $this->widener);
    }

    /**
     * @param array<array-key, mixed> $row
     */
    public function observe(array $row): void
    {
        /** @var mixed $value */
        foreach ($row as $name => $value) {
            $current = array_key_exists($name, $this->types) ? $this->types[$name] : null;

            if (
                $value !== null
                && (
                    $current instanceof StringType
                    || $current instanceof OptionalType
                    && $current->base() instanceof StringType
                )
            ) {
                continue;
            }

            $observed = match (true) {
                $value === null => type_null(),
                is_string($value) && trim($value) !== $value => type_string(),
                default => $this->typer->narrow($value),
            };

            if ($value !== null && $observed instanceof NullType) {
                $observed = type_string();
            }

            $this->types[$name] = $current === null ? $observed : $this->widener->widen($current, $observed);
        }

        $this->rows++;
    }

    public function rows(): int
    {
        return $this->rows;
    }
}
