<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Schema;
use Flow\ETL\{SchemaValidator};

/**
 * Matches all entries in the schema, if row comes with any extra entry it will fail validation.
 */
final class StrictValidator implements SchemaValidator
{
    public function isValid(Schema $given, Schema $expected) : bool
    {
        if ($expected->count() !== $given->count()) {
            return false;
        }

        foreach ($given->definitions() as $givenDefinition) {
            $definition = $expected->findDefinition($givenDefinition->entry());

            if ($definition === null) {
                return false;
            }

            if ($definition->isNullable() && $givenDefinition->metadata()->has(Metadata::FROM_NULL)) {
                return true;
            }

            if (!$definition->isCompatible($givenDefinition)) {
                return false;
            }
        }

        return true;
    }
}
