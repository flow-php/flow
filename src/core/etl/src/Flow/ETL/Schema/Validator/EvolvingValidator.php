<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\SchemaValidator;

/**
 * Compatibility for combining or evolving datasets - the same invariant a merge,
 * an append and a per-batch validation all need: combining `expected` and `given`
 * must never leave a row holding null in a column that is non-nullable somewhere.
 *
 * - a shared column must keep a compatible type and must not widen to nullable
 *   where the expected side is non-nullable (its rows would then read as null),
 * - a column in `expected` but absent from `given` is allowed only when nullable
 *   (given's rows read it as null),
 * - a column in `given` but absent from `expected` is allowed only when nullable
 *   (expected's rows read it as null).
 *
 * A required (non-nullable) column that is dropped, added, or type-changed is
 * therefore incompatible.
 */
final class EvolvingValidator implements SchemaValidator
{
    public function validate(Schema $expected, Schema $given): ValidationContext
    {
        $missingDefinitions = [];
        $mismatchedDefinitions = [];
        $unexpectedDefinitions = [];

        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
                if (!$expectedDefinition->isNullable()) {
                    $missingDefinitions[] = $expectedDefinition;
                }

                continue;
            }

            // B27: an all-null batch infers NullDefinition; against a nullable expectation that is a
            // representable column, and the other two validators already say so
            if ($givenDefinition instanceof NullDefinition && $expectedDefinition->isNullable()) {
                continue;
            }

            if (!$expectedDefinition->isCompatible($givenDefinition)) {
                $mismatchedDefinitions[] = new MismatchedDefinition($expectedDefinition, $givenDefinition);
            }
        }

        foreach ($given->definitions() as $givenDefinition) {
            if ($expected->findDefinition($givenDefinition->entry()) === null && !$givenDefinition->isNullable()) {
                $unexpectedDefinitions[] = $givenDefinition;
            }
        }

        return new ValidationContext($missingDefinitions, $mismatchedDefinitions, $unexpectedDefinitions);
    }
}
