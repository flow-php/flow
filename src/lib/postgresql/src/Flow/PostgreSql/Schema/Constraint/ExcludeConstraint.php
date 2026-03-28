<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

final readonly class ExcludeConstraint
{
    public function __construct(
        public string $definition,
        public ?string $name = null,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->definition === $other->definition;
    }
}
