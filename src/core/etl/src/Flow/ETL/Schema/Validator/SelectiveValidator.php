<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\SchemaValidator;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

/**
 * Matches only entries defined in the expected schema allowing for extra entries in given schema.
 */
final class SelectiveValidator implements SchemaValidator
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

        return new ValidationContext($missingDefinitions, $mismatchedDefinitions);
    }
}
