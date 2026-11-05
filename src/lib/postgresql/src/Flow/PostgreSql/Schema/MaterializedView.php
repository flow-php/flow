<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Sql;

use function array_map;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\parsed_select;

/**
 * @import-type IndexShape from Index
 *
 * @type MaterializedViewShape = array{name: string, definition: string, indexes: list<IndexShape>, schema?: ?string}
 */
final readonly class MaterializedView
{
    /**
     * @param list<Index> $indexes
     */
    public function __construct(
        public string $name,
        public string $definition,
        public array $indexes = [],
        public ?string $schema = null,
    ) {}

    /**
     * @param MaterializedViewShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            definition: $data['definition'],
            indexes: array_map(static fn(array $index): Index => Index::fromArray($index), $data['indexes']),
            schema: $data['schema'] ?? null,
        );
    }

    /**
     * @return MaterializedViewShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'definition' => $this->definition,
            'indexes' => array_map(static fn(Index $index): array => $index->normalize(), $this->indexes),
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

    /**
     * @return list<Sql>
     */
    public function toSql(): array
    {
        $sqls = [];

        $sqls[] = create()->materializedView($this->name, $this->schemaName())->as(parsed_select($this->definition));

        foreach ($this->indexes as $idx) {
            $sqls[] = $idx->toSql($this->name, $this->schemaName());
        }

        return $sqls;
    }

    public function withSchema(string $schema): self
    {
        return new self($this->name, $this->definition, $this->indexes, $schema);
    }
}
