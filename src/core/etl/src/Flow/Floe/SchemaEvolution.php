<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema;
use Flow\Floe\Exception\IncompatibleSchemaException;

use function sprintf;

/**
 * Validates the schema of an appended section against the file's merged schema:
 * existing columns must keep name and compatible type, new columns must be
 * nullable, omitted columns must be nullable in the file schema.
 */
final class SchemaEvolution
{
    /**
     * @throws IncompatibleSchemaException
     */
    public function validate(Schema $fileSchema, Schema $incoming): void
    {
        foreach ($incoming->definitions() as $incomingDefinition) {
            $name = $incomingDefinition->entry()->name();
            $existing = $fileSchema->findDefinition($name);

            if ($existing === null) {
                if (!$incomingDefinition->isNullable()) {
                    throw new IncompatibleSchemaException(sprintf(
                        'Floe append adds new column "%s" which must be nullable - rows already in the file hold null for it',
                        $name,
                    ));
                }

                continue;
            }

            if (!$existing->isCompatible($incomingDefinition)) {
                throw new IncompatibleSchemaException(sprintf(
                    'Floe append changes column "%s" from %s to %s which is not compatible',
                    $name,
                    $existing->type()->toString(),
                    $incomingDefinition->type()->toString(),
                ));
            }
        }

        foreach ($fileSchema->definitions() as $existingDefinition) {
            $name = $existingDefinition->entry()->name();

            if ($incoming->findDefinition($name) === null && !$existingDefinition->isNullable()) {
                throw new IncompatibleSchemaException(sprintf(
                    'Floe append omits column "%s" which is not nullable in the file schema',
                    $name,
                ));
            }
        }
    }
}
