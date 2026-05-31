<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

use function str_ends_with;

final readonly class EndsWithExclusionPolicy implements ExclusionPolicy
{
    public function __construct(
        private string $suffix,
    ) {}

    public function exclude(SchemaObject $object): bool
    {
        return $object->name !== null && str_ends_with($object->name, $this->suffix);
    }
}
