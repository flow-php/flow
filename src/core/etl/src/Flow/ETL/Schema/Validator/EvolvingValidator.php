<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema;
use Flow\ETL\{SchemaValidator};

/**
 * Rules of evolving schema matching:
 * - if schemas are the same, return true
 * - if given schema has less fields than expected schema, return false
 * - if given schema is making a nullable field non-nullable, return false
 * - if given schema is making a non-nullable field nullable, return true
 * - if given schema is changing the type of a field, return false
 * - if given schema is adding a field, return true
 */
final class EvolvingValidator implements SchemaValidator
{
    public function isValid(Schema $expected, Schema $given) : bool
    {
        if ($given->count() < $expected->count()) {
            return false;
        }

        foreach ($expected->definitions() as $definition) {
            if ($given->findDefinition($definition->entry()) === null) {
                return false;
            }
        }

        foreach ($given->definitions() as $rightDefinition) {
            $leftDefinition = $expected->findDefinition($rightDefinition->entry());

            if ($leftDefinition === null) {
                continue;
            }

            if (!$rightDefinition->isNullable() && $leftDefinition->isNullable()) {
                return false;
            }

            // making both sides nullable to compare just types of the fields
            if (!$rightDefinition->type()->makeNullable(true)->isEqual($leftDefinition->type()->makeNullable(true))) {
                return false;
            }
        }

        return true;
    }
}
