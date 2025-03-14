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

        foreach ($given->definitions() as $definition) {
            $expectedDefinition = $expected->findDefinition($definition->entry());

            if ($expectedDefinition === null) {
                return false;
            }

            if (!$expectedDefinition->isCompatible($definition)) {
                return false;
            }
        }

        return true;
    }
}
