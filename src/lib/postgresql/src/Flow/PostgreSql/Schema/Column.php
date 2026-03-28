<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

final readonly class Column
{
    public function __construct(
        public string $name,
        public ColumnType $type,
        public bool $nullable,
        public ?string $default = null,
        public bool $isIdentity = false,
        public ?IdentityGeneration $identityGeneration = null,
        public bool $isGenerated = false,
        public ?string $generationExpression = null,
        public ?int $ordinalPosition = null,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->type->isEqual($other->type)
            && $this->nullable === $other->nullable
            && $this->default === $other->default
            && $this->isIdentity === $other->isIdentity
            && $this->identityGeneration === $other->identityGeneration
            && $this->isGenerated === $other->isGenerated
            && $this->generationExpression === $other->generationExpression;
    }
}
