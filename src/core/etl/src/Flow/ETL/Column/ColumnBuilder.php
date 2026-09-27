<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\SchemaMismatchException;

interface ColumnBuilder
{
    /**
     * A logical or raw value: Type::cast, then its physical form; null is validity 0, or
     * ColumnMismatchException::valueDoesNotMatch() on a NOT NULL definition.
     */
    public function append(mixed $value): void;

    /**
     * @param list<mixed> $values one cast loop per column - the bulk door
     *
     * @throws SchemaMismatchException at the first refused value, its row the value's position in $values
     */
    public function appendMany(array $values): void;

    /**
     * Copies one physical cell, no materialisation.
     */
    public function appendFrom(Column $column, int $i): void;

    /**
     * @param list<int> $indices gathered physically from $column
     */
    public function appendTake(Column $column, array $indices): void;

    public function count(): int;

    public function finish(): Column;
}
