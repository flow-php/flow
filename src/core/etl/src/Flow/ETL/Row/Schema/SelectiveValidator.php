<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

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

            if (!$givenDefinition) {
                return false;
            }

            if (!$givenDefinition->isEqual($expectedDefinition)) {
                return false;
            }
        }

        return true;
    }
}
