<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

final readonly class ExactMatchExclusionPolicy implements ExclusionPolicy
{
    public function __construct(
        private string $name,
    ) {}

    public function exclude(SchemaObject $object): bool
    {
        return $object->name === $this->name;
    }
}
