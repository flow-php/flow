<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\TypeNarrower;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function is_string;
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
     * Merge left-to-right in listing order: name order follows first-seen, so a different bracketing of the name
     * sequence changes the definition order (never the types).
     */
    public function merge(self $other, bool $unionByName): self
    {
        $merged = new self([], $this->typer, $this->widener);
        $merged->types = $this->types;
        $merged->rows = $this->rows + $other->rows;

        foreach ($other->types as $name => $type) {
            if (!array_key_exists($name, $merged->types)) {
                if (!$unionByName) {
                    continue;
                }

                $merged->types[$name] = $type;

                continue;
            }

            $merged->types[$name] = $this->widener->widen($merged->types[$name], $type);
        }

        return $merged;
    }

    /**
     * RawRowValues::$metadata is not read: the fold types columns, metadata is folded by the hydrator per batch.
     */
    public function observe(RawRowValues $row): void
    {
        /** @var mixed $value */
        foreach ($row->values as $name => $value) {
            $observed = match (true) {
                $value === null => type_null(),
                is_string($value) && trim($value) !== $value => type_string(),
                default => $this->typer->narrow($value),
            };

            if ($value !== null && $observed instanceof NullType) {
                $observed = type_string();
            }

            $this->types[$name] = array_key_exists($name, $this->types)
                ? $this->widener->widen($this->types[$name], $observed)
                : $observed;
        }

        $this->rows++;
    }

    public function rows(): int
    {
        return $this->rows;
    }

    public function schema(TypeFloor $floor): Schema
    {
        $definitions = [];

        foreach ($this->types as $name => $type) {
            $definitions[] = definition_from_type((string) $name, $floor->floor($type), nullable: true);
        }

        return new Schema(...$definitions);
    }
}
