<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use Flow\PostgreSql\Parser\ExcludeDefinitionParser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Parser\ParsedExcludeDefinition;

/**
 * @type ExcludeConstraintShape = array{definition: string, name: ?string}
 */
final readonly class ExcludeConstraint
{
    private ParsedExcludeDefinition $parsed;

    public function __construct(
        public string $definition,
        public ?string $name = null,
    ) {
        $this->parsed = (new ExcludeDefinitionParser(new ExpressionParser()))->parse($definition);
    }

    /**
     * @param ExcludeConstraintShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(definition: $data['definition'], name: $data['name'] ?? null);
    }

    public function isEqual(self $other): bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other): bool
    {
        return $this->parsed->equals($other->parsed);
    }

    /**
     * @return ExcludeConstraintShape
     */
    public function normalize(): array
    {
        return [
            'definition' => $this->definition,
            'name' => $this->name,
        ];
    }
}
