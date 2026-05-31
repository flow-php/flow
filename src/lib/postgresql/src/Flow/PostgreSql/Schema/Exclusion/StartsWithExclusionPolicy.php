<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

use function str_starts_with;

final readonly class StartsWithExclusionPolicy implements ExclusionPolicy
{
    public function __construct(
        private string $prefix,
    ) {}

    public function exclude(SchemaObject $object): bool
    {
        return $object->name !== null && str_starts_with($object->name, $this->prefix);
    }
}
