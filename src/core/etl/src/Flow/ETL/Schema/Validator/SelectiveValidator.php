<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use function Flow\Types\DSL\{type_equals, type_string};
use Flow\ETL\Schema;
use Flow\ETL\{SchemaValidator, Schema\Metadata};

/**
 * Matches only entries defined in the expected schema allowing for extra entries in given schema.
 */
final class SelectiveValidator implements SchemaValidator
{
    public function isValid(Schema $expected, Schema $given) : bool
    {
        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
                return false;
            }

            if ($expectedDefinition->isNullable() && $givenDefinition->metadata()->has(Metadata::FROM_NULL) && type_equals($givenDefinition->type(), type_string())) {
                continue;
            }

            if (!$expectedDefinition->isCompatible($givenDefinition)) {
                return false;
            }
        }

        return true;
    }
}
