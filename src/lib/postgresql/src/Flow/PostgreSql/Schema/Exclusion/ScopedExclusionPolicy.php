<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

final readonly class ScopedExclusionPolicy implements ExclusionPolicy
{
    /**
     * @param ?SchemaObjectType $type limit the wrapped policy to a single object type, null matches any type
     * @param ?string $schema limit the wrapped policy to a single schema, null matches any schema
     */
    public function __construct(
        private ExclusionPolicy $inner,
        private ?SchemaObjectType $type = null,
        private ?string $schema = null,
    ) {}

    public function exclude(SchemaObject $object): bool
    {
        if ($this->type !== null && $object->type !== $this->type) {
            return false;
        }

        if ($this->schema !== null && $object->schema !== $this->schema) {
            return false;
        }

        return $this->inner->exclude($object);
    }
}
