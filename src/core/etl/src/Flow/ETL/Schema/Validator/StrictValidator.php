<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Schema;
use Flow\ETL\{SchemaValidator, Schema\Metadata};

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
            $expectedDefinition = $expected->findDefinition($givenDefinition->entry());

            if ($expectedDefinition === null) {
                return false;
            }

            if ($expectedDefinition->isNullable() && $givenDefinition->metadata()->has(Metadata::FROM_NULL) && $givenDefinition->type()->isSame(type_string(true))) {
                continue;
            }

            if (!$expectedDefinition->isCompatible($givenDefinition)) {
                return false;
            }
        }

        return true;
    }
}
