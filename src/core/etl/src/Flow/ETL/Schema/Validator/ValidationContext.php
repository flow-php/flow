<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Validator;

use Flow\ETL\Schema\Definition;

final readonly class ValidationContext
{
    /**
     * @param array<Definition<mixed>> $missingDefinitions - definitions expected but absent in the given schema
     * @param array<MismatchedDefinition> $mismatchedDefinitions - definitions rejected by the validator
     * @param array<Definition<mixed>> $unexpectedDefinitions - definitions present in the given schema but not expected
     */
    public function __construct(
        private array $missingDefinitions = [],
        private array $mismatchedDefinitions = [],
        private array $unexpectedDefinitions = [],
    ) {}

    public function isValid(): bool
    {
        return (
            $this->missingDefinitions === []
            && $this->mismatchedDefinitions === []
            && $this->unexpectedDefinitions === []
        );
    }

    /**
     * @return array<MismatchedDefinition>
     */
    public function mismatchedDefinitions(): array
    {
        return $this->mismatchedDefinitions;
    }

    public function toString(): string
    {
        if ($this->isValid()) {
            return '';
        }

        $message = '';

        if ($this->missingDefinitions !== []) {
            $message .= "  Missing Definitions: \n";

            foreach ($this->missingDefinitions as $missingDefinition) {
                $message .= '    |-- ' . $this->definitionToString($missingDefinition) . "\n";
            }
        }

        if ($this->mismatchedDefinitions !== []) {
            $message .= "  Mismatched Definitions: \n";

            foreach ($this->mismatchedDefinitions as $mismatchedDefinition) {
                $message .=
                    '    |-- expected: '
                    . $this->definitionToString($mismatchedDefinition->expected())
                    . ', given: '
                    . $this->definitionToString($mismatchedDefinition->given())
                    . "\n";
            }
        }

        if ($this->unexpectedDefinitions !== []) {
            $message .= "  Unexpected Definitions: \n";

            foreach ($this->unexpectedDefinitions as $unexpectedDefinition) {
                $message .= '    |-- ' . $this->definitionToString($unexpectedDefinition) . "\n";
            }
        }

        return $message;
    }

    /**
     * @return array<Definition<mixed>>
     */
    public function missingDefinitions(): array
    {
        return $this->missingDefinitions;
    }

    /**
     * @return array<Definition<mixed>>
     */
    public function unexpectedDefinitions(): array
    {
        return $this->unexpectedDefinitions;
    }

    /**
     * @param Definition<mixed> $definition
     */
    private function definitionToString(Definition $definition): string
    {
        return (
            $definition->entry()->name()
            . '<'
            . ($definition->isNullable() ? '?' : '')
            . $definition->type()->toString()
            . '>'
        );
    }
}
