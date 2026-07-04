<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\SchemaValidator;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

/**
 * Matches all entries in the schema, if row comes with any extra entry it will fail validation.
 */
final class StrictValidator implements SchemaValidator
{
    public function validate(Schema $expected, Schema $given): ValidationContext
    {
        $missingDefinitions = [];
        $mismatchedDefinitions = [];
        $unexpectedDefinitions = [];

        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
                $missingDefinitions[] = $expectedDefinition;

                continue;
            }

            if (
                $expectedDefinition->isNullable()
                && $givenDefinition->metadata()->has(Metadata::FROM_NULL)
                && type_equals($givenDefinition->type(), type_string())
            ) {
                continue;
            }

            if (!$expectedDefinition->isCompatible($givenDefinition)) {
                $mismatchedDefinitions[] = new MismatchedDefinition($expectedDefinition, $givenDefinition);
            }
        }

        foreach ($given->definitions() as $givenDefinition) {
            if ($expected->findDefinition($givenDefinition->entry()) === null) {
                $unexpectedDefinitions[] = $givenDefinition;
            }
        }

        return new ValidationContext($missingDefinitions, $mismatchedDefinitions, $unexpectedDefinitions);
    }
}
