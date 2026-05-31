<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

final readonly class WholeSchemaExclusionPolicy implements ExclusionPolicy
{
    public function __construct(
        private string $schema,
    ) {}

    public function exclude(SchemaObject $object): bool
    {
        return $object->schema === $this->schema;
    }
}
