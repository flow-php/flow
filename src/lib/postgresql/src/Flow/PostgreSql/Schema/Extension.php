<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\create;

/**
 * @type ExtensionShape = array{name: string, version?: ?string, schema?: string}
 */
final readonly class Extension
{
    public function __construct(
        public string $name,
        public ?string $version = null,
        public string $schema = 'public',
    ) {}

    /**
     * @param ExtensionShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(name: $data['name'], version: $data['version'] ?? null, schema: $data['schema'] ?? 'public');
    }

    /**
     * @return ExtensionShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'version' => $this->version,
            'schema' => $this->schema,
        ];
    }

    public function toSql(): Sql
    {
        $builder = create()->extension($this->name)->schema($this->schema);

        if ($this->version !== null) {
            $builder = $builder->version($this->version);
        }

        return $builder;
    }
}
