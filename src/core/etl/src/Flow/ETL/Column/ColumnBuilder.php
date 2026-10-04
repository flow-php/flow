<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;

use function interface_exists;

if (interface_exists(ColumnBuilder::class, false)) {
    return;
}

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
     * Physical values exactly as Column::physicals() returns them - no cast, no conversion.
     *
     * @param list<mixed> $physicals
     * @param null|int $nullCount exactly the nulls among $physicals when the caller counted them, null otherwise - a
     *                            count that is not exact is undefined behaviour
     *
     * @throws SchemaMismatchException at the first null under a NOT NULL definition
     * @throws InvalidArgumentException for a physical of another kind than the definition's (a string in an
     *                                  integer column, an int in a float column); nothing is appended
     */
    public function appendPhysicals(array $physicals, ?int $nullCount = null): void;

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
