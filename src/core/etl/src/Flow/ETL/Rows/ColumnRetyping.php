<?php

declare(strict_types=1);

namespace Flow\ETL\Rows;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Retype;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Schema\Definition;

/**
 * One column brought under another definition of the same name: Rows::matchTo() validates the values the types cannot
 * prove, a declared schema over another frame's batch casts them.
 */
final readonly class ColumnRetyping
{
    /**
     * The column a batch of $count rows lacks: nulls, or refused under NOT NULL when there is a row to refuse.
     *
     * @param Definition<mixed> $definition
     *
     * @throws SchemaMismatchException
     */
    public function absent(Definition $definition, int $count, Backend $backend): Column
    {
        if (!$definition->isNullable()) {
            if ($count > 0) {
                throw new SchemaMismatchException(0, ColumnMismatchException::missingColumn($definition));
            }

            return $backend->builder($definition)->finish();
        }

        return $backend->constant($definition, null, $count);
    }

    /**
     * $column under $definition for a declared schema over another frame's batch: restamped when the types prove the
     * change, otherwise cast value by value into $backend - a value the definition does not take is cast, not refused.
     * Contrast validate(), which refuses it.
     *
     * @param Definition<mixed> $own
     * @param Definition<mixed> $definition
     *
     * @throws SchemaMismatchException a value the cast refuses, at its row
     */
    public function cast(Column $column, Definition $own, Definition $definition, Backend $backend): Column
    {
        return $this->restamped($column, $own, $definition) ?? $this->rebuilt($column, $definition, $backend);
    }

    /**
     * Refuses the first null of $column when $definition is NOT NULL.
     *
     * @param Definition<mixed> $definition
     *
     * @throws SchemaMismatchException
     */
    public function notNull(Definition $definition, Column $column): void
    {
        if ($definition->isNullable() || $column->nullCount() === 0) {
            return;
        }

        $index = 0;

        while (!$column->isNull($index)) {
            $index++;
        }

        throw new SchemaMismatchException($index, ColumnMismatchException::valueDoesNotMatch($definition, null));
    }

    /**
     * $column's values built anew under $definition in $backend.
     *
     * @param Definition<mixed> $definition
     *
     * @throws SchemaMismatchException
     */
    public function rebuilt(Column $column, Definition $definition, Backend $backend): Column
    {
        $builder = $backend->builder($definition);
        $builder->appendMany($column->values());

        return $builder->finish();
    }

    /**
     * $column as it is when $own is $definition, restamped when the types prove the change and no null breaks a NOT NULL
     * definition, null otherwise - the caller then casts or validates it.
     *
     * @param Definition<mixed> $own
     * @param Definition<mixed> $definition
     */
    public function restamped(Column $column, Definition $own, Definition $definition): ?Column
    {
        if ($own->isSame($definition)) {
            return $column;
        }

        if (!$definition->isNullable() && $column->nullCount() > 0) {
            return null;
        }

        return (new Retype())->proves($column->type(), $definition->type())
            ? $column->withType($definition->type())
            : null;
    }

    /**
     * Rows::matchTo()'s check of the columns restamped() could not prove: every value must already be one its
     * definition takes - nothing is cast. One row-major scan, so the refusal names the lowest bad row and, within it,
     * the first column in the given order. Contrast cast(), which converts such values instead.
     *
     * @param array<string, array{Column, Definition<mixed>}> $changed by name, in schema order
     *
     * @throws SchemaMismatchException
     */
    public function validate(array $changed, int $count): void
    {
        for ($i = 0; $i < $count; $i++) {
            foreach ($changed as [$column, $definition]) {
                // @mago-ignore analysis:mixed-assignment
                $value = $column->value($i);

                if ($value === null ? !$definition->isNullable() : !$definition->matches($value)) {
                    throw new SchemaMismatchException($i, ColumnMismatchException::valueDoesNotMatch(
                        $definition,
                        $value,
                    ));
                }
            }
        }
    }
}
