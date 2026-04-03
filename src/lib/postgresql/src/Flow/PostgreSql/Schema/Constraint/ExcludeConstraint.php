<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

/**
 * @phpstan-type ExcludeConstraintShape = array{definition: string, name: ?string}
 */
final readonly class ExcludeConstraint
{
    public function __construct(
        public string $definition,
        public ?string $name = null,
    ) {
    }

    /**
     * @param ExcludeConstraintShape $data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            definition: $data['definition'],
            name: $data['name'] ?? null,
        );
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->definition === $other->definition;
    }

    /**
     * @return ExcludeConstraintShape
     */
    public function normalize() : array
    {
        return [
            'definition' => $this->definition,
            'name' => $this->name,
        ];
    }
}
