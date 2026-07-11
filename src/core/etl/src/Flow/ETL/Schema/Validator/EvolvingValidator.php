<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema;
use Flow\ETL\SchemaValidator;

use function Flow\Types\DSL\type_equals;

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
    public function validate(Schema $expected, Schema $given): ValidationContext
    {
        $missingDefinitions = [];
        $mismatchedDefinitions = [];

        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
                $missingDefinitions[] = $expectedDefinition;

                continue;
            }

            if (!$givenDefinition->isNullable() && $expectedDefinition->isNullable()) {
                $mismatchedDefinitions[] = new MismatchedDefinition($expectedDefinition, $givenDefinition);

                continue;
            }

            if (!type_equals($givenDefinition->type(), $expectedDefinition->type())) {
                $mismatchedDefinitions[] = new MismatchedDefinition($expectedDefinition, $givenDefinition);
            }
        }

        return new ValidationContext($missingDefinitions, $mismatchedDefinitions);
    }
}
