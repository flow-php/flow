<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Schema\Exception\SchemaException;

use function array_key_exists;
use function array_map;
use function array_values;
use function sprintf;

/**
 * @import-type SchemaShape from Schema
 *
 * @type CatalogShape = array{schemas: list<SchemaShape>}
 */
final readonly class Catalog
{
    /**
     * @param list<Schema> $schemas
     */
    public function __construct(
        private array $schemas,
    ) {}

    /**
     * @param CatalogShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(schemas: array_map(static fn(array $s): Schema => Schema::fromArray($s), $data['schemas']));
    }

    /**
     * @return list<Schema>
     */
    public function all(): array
    {
        return $this->schemas;
    }

    public function get(string $name): Schema
    {
        foreach ($this->schemas as $schema) {
            if ($schema->name === $name) {
                return $schema;
            }
        }

        throw new SchemaException(sprintf('Schema "%s" not found in catalog.', $name));
    }

    public function has(string $name): bool
    {
        foreach ($this->schemas as $schema) {
            if ($schema->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function merge(self $other): self
    {
        $schemas = [];

        foreach ($this->schemas as $schema) {
            $schemas[$schema->name] = $schema;
        }

        foreach ($other->schemas as $schema) {
            $schemas[$schema->name] = array_key_exists($schema->name, $schemas)
                ? $schemas[$schema->name]->merge($schema)
                : $schema;
        }

        return new self(array_values($schemas));
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_map(static fn(Schema $s): string => $s->name, $this->schemas);
    }

    /**
     * @return CatalogShape
     */
    public function normalize(): array
    {
        return [
            'schemas' => array_map(static fn(Schema $s): array => $s->normalize(), $this->schemas),
        ];
    }
}
