<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use Flow\PostgreSql\Parser\ExpressionParser;

/**
 * @type CheckConstraintShape = array{expression: string, name: ?string, no_inherit: bool}
 */
final readonly class CheckConstraint
{
    public string $expression;

    public function __construct(
        string $expression,
        public ?string $name = null,
        public bool $noInherit = false,
    ) {
        $this->expression = (new ExpressionParser())->normalize($expression);
    }

    /**
     * @param CheckConstraintShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(expression: $data['expression'], name: $data['name'] ?? null, noInherit: $data['no_inherit']);
    }

    public function isEqual(self $other): bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other): bool
    {
        return $this->expression === $other->expression && $this->noInherit === $other->noInherit;
    }

    /**
     * @return CheckConstraintShape
     */
    public function normalize(): array
    {
        return [
            'expression' => $this->expression,
            'name' => $this->name,
            'no_inherit' => $this->noInherit,
        ];
    }
}
