<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

final readonly class SchemaObject
{
    /**
     * @param ?string $name name of the object within the schema, null when $type is SchemaObjectType::SCHEMA
     */
    public function __construct(
        public SchemaObjectType $type,
        public string $schema,
        public ?string $name = null,
    ) {}
}
