<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Schema\Exception\SchemaException;

final readonly class Catalog
{
    /**
     * @param list<Schema> $schemas
     */
    public function __construct(
        private array $schemas,
    ) {
    }

    /**
     * @return list<Schema>
     */
    public function all() : array
    {
        return $this->schemas;
    }

    public function get(string $name) : Schema
    {
        foreach ($this->schemas as $schema) {
            if ($schema->name === $name) {
                return $schema;
            }
        }

        throw new SchemaException(\sprintf('Schema "%s" not found in catalog.', $name));
    }

    public function has(string $name) : bool
    {
        foreach ($this->schemas as $schema) {
            if ($schema->name === $name) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return list<string>
     */
    public function names() : array
    {
        return \array_map(
            static fn (Schema $s) : string => $s->name,
            $this->schemas,
        );
    }
}
