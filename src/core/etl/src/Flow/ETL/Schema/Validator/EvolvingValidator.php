<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use function Flow\Types\DSL\type_equals;
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
    /**
     * @param Schema $expected
     * @param Schema $given
     */
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

            if (!type_equals($rightDefinition->type(), $leftDefinition->type())) {
                return false;
            }
        }

        return true;
    }
}
