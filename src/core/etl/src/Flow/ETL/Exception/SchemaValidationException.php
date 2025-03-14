<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row\Schema;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(private readonly Schema $expected, private readonly Schema $given)
    {
        /**
         * @var array<string> $missingDefinitions
         */
        $missingDefinitions = [];

        /**
         * @var array<string> $mismatchedDefinitions
         */
        $mismatchedDefinitions = [];

        /**
         * @var array<string> $unexpectedDefinitions
         */
        $unexpectedDefinitions = [];

        foreach ($this->expected->definitions() as $expectedDefinition) {
            $givenDefinition = $this->given->findDefinition($expectedDefinition->entry());

            if ($givenDefinition === null) {
                $missingDefinitions[] = $expectedDefinition->entry() . '<' . $expectedDefinition->type()->toString() . '>';

                continue;
            }

            if (!$expectedDefinition->isEqual($givenDefinition)) {
                $mismatchedDefinitions[] = 'expected: ' . $expectedDefinition->entry()->name() . '<' . $expectedDefinition->type()->toString() . '>, ' .
                    'given: ' . $givenDefinition->entry()->name() . '<' . $givenDefinition->type()->toString() . '>';
            }
        }

        foreach ($this->given->definitions() as $givenDefinition) {
            if ($this->expected->findDefinition($givenDefinition->entry()) === null) {
                $unexpectedDefinitions[] = $givenDefinition->entry() . '<' . $givenDefinition->type()->toString() . '>';
            }
        }

        $message = '';

        if (\count($missingDefinitions)) {
            $message .= "  Missing Definitions: \n";

            foreach ($missingDefinitions as $missingDefinition) {
                $message .= '    |-- ' . $missingDefinition . "\n";
            }
        }

        if (\count($mismatchedDefinitions)) {
            $message .= "  Mismatched Definitions: \n";

            foreach ($mismatchedDefinitions as $mismatchedDefinition) {
                $message .= '    |-- ' . $mismatchedDefinition . "\n";
            }
        }

        if (\count($unexpectedDefinitions)) {
            $message .= "  Unexpected Definitions: \n";

            foreach ($unexpectedDefinitions as $unexpectedDefinition) {
                $message .= '    |-- ' . $unexpectedDefinition . "\n";
            }
        }

        parent::__construct("Schema validation failed: \n" . $message);
    }

    public function given() : Schema
    {
        return $this->given;
    }

    public function schema() : Schema
    {
        return $this->expected;
    }
}
