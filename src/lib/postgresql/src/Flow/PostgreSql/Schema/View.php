<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\parsed_select;

/**
 * @type ViewShape = array{name: string, definition: string, is_updatable: bool, schema?: ?string}
 */
final readonly class View
{
    public function __construct(
        public string $name,
        public string $definition,
        public bool $isUpdatable = false,
        public ?string $schema = null,
    ) {}

    /**
     * @param ViewShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            definition: $data['definition'],
            isUpdatable: $data['is_updatable'],
            schema: $data['schema'] ?? null,
        );
    }

    /**
     * @return ViewShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'definition' => $this->definition,
            'is_updatable' => $this->isUpdatable,
            'schema' => $this->schema,
        ];
    }

    public function qualifiedName(): string
    {
        return $this->schemaName() . '.' . $this->name;
    }

    public function schemaName(): string
    {
        return $this->schema ?? 'public';
    }

    public function toSql(): Sql
    {
        return create()->view($this->name, $this->schemaName())->as(parsed_select($this->definition));
    }

    public function withSchema(string $schema): self
    {
        return new self($this->name, $this->definition, $this->isUpdatable, $schema);
    }
}
