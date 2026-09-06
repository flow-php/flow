<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\Exception as TypesException;

use function array_key_exists;

/**
 * Builds a batch against a declared Schema. Metadata belongs to the column, so it is folded into the
 * schema once - a per-row divergent Metadata is no longer representable.
 */
final readonly class HydratedBatch
{
    /**
     * @param list<RawRowValues> $batch
     *
     * @throws SchemaMismatchException
     */
    public function of(array $batch, Schema $schema): Rows
    {
        // Nullability is a Definition flag, never a property of a Type: definition_from_type()
        // unwraps OptionalType into nullable: true over a BARE type, so $definition->type() is
        // non-nullable even for a nullable column and would refuse every legitimate null.
        $prepare = static fn(mixed $value, Definition $definition): mixed => (
            $value === null
                ? ($definition->isNullable() ? null : throw new CastingException(null, $definition->type()))
                : $definition->type()->cast($value)
        );

        foreach ($batch as $rowValues) {
            foreach ($rowValues->metadata as $name => $metadata) {
                // PHP casts a numeric array key to int, column names are always strings
                $name = (string) $name;

                if ($schema->findDefinition($name) !== null) {
                    $schema = $schema->setMetadata($name, $metadata);
                }
            }
        }

        $definitions = $schema->definitions();
        $rows = [];

        foreach ($batch as $rowIndex => $rowValues) {
            $values = [];

            foreach ($definitions as $definition) {
                $name = $definition->entry()->name();

                if (!array_key_exists($name, $rowValues->values)) {
                    // never fill: Rows::__construct runs Row::matchTo(), which pads a declared-nullable
                    // absence and refuses a NOT-NULL one as a missing column rather than as a null value
                    continue;
                }

                try {
                    $values[$name] = $prepare($rowValues->values[$name], $definition);
                } catch (TypesException) {
                    // Rows' gate reports a misfit this way, but $prepare runs before the Rows
                    // constructor, so the batch has to place the violation itself.
                    throw new SchemaMismatchException($rowIndex, ColumnMismatchException::valueDoesNotMatch(
                        $definition,
                        $rowValues->values[$name],
                    ));
                }
            }

            $rows[] = new Row($values);
        }

        return new Rows($schema, ...$rows);
    }
}
