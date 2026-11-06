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
 * @import-type ExtensionShape from Extension
 *
 * @type CatalogShape = array{schemas: list<SchemaShape>, extensions?: list<ExtensionShape>}
 */
final readonly class Catalog
{
    /**
     * @param list<Schema> $schemas
     * @param list<Extension> $extensions
     */
    public function __construct(
        private array $schemas,
        private array $extensions = [],
    ) {
        foreach ($extensions as $extension) {
            if (!$this->has($extension->schema)) {
                throw new SchemaException(sprintf(
                    'Extension "%s" is placed in schema "%s", which the catalog does not declare; add schema("%s") to catalog().',
                    $extension->name,
                    $extension->schema,
                    $extension->schema,
                ));
            }
        }
    }

    /**
     * @param CatalogShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            schemas: array_map(static fn(array $s): Schema => Schema::fromArray($s), $data['schemas']),
            extensions: array_map(static fn(array $e): Extension => Extension::fromArray(
                $e,
            ), $data['extensions'] ?? []),
        );
    }

    /**
     * @return list<Schema>
     */
    public function all(): array
    {
        return $this->schemas;
    }

    /**
     * @return list<Extension>
     */
    public function extensions(): array
    {
        return $this->extensions;
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

        $extensions = [];

        foreach ($this->extensions as $extension) {
            $extensions[$extension->name] = $extension;
        }

        foreach ($other->extensions as $extension) {
            $extensions[$extension->name] = $extension;
        }

        return new self(array_values($schemas), array_values($extensions));
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
            'extensions' => array_map(static fn(Extension $e): array => $e->normalize(), $this->extensions),
        ];
    }
}
