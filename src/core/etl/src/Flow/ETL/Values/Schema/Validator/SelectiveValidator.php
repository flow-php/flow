<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Validator;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use Flow\ETL\Values\SchemaValidator;
use SensitiveParameter;

/**
 * Matches only columns defined in the expected schema allowing for extra columns in given schema.
 */
final class SelectiveValidator implements Describable, SchemaValidator
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function validate(Schema $expected, Schema $given): ValidationContext
    {
        $missingDefinitions = [];
        $mismatchedDefinitions = [];

        foreach ($expected->definitions() as $expectedDefinition) {
            $givenDefinition = $given->findDefinition($expectedDefinition->name());

            if ($givenDefinition === null) {
                $missingDefinitions[] = $expectedDefinition;

                continue;
            }

            if ($givenDefinition instanceof NullDefinition && $expectedDefinition->isNullable()) {
                continue;
            }

            if (!$expectedDefinition->isCompatible($givenDefinition)) {
                $mismatchedDefinitions[] = new MismatchedDefinition($expectedDefinition, $givenDefinition);
            }
        }

        return new ValidationContext($missingDefinitions, $mismatchedDefinitions);
    }
}
