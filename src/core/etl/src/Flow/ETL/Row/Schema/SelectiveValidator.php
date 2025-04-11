<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Row\Schema;
use Flow\ETL\{SchemaValidator};

/**
 * Matches only entries defined in the expected schema allowing for extra entries in given schema.
 */
final class SelectiveValidator implements SchemaValidator
{
    public function isValid(Schema $given, Schema $expected) : bool
    {
        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
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
