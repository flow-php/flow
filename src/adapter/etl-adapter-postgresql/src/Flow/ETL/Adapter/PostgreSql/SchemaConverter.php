<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\TableOptions;
use Flow\Types\Type;

use function array_keys;
use function array_map;
use function array_search;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function implode;
use function in_array;
use function is_int;
use function str_starts_with;
use function strlen;
use function substr;
use function usort;

/**
 * Converts between a Flow {@see Schema} and a PostgreSQL {@see Table}.
 */
final readonly class SchemaConverter
{
    private EntryTypesMap $typesMap;

    public function __construct(?EntryTypesMap $typesMap = null)
    {
        $this->typesMap = $typesMap ?? new EntryTypesMap();
    }

    public function toFlowSchema(Table $table): Schema
    {
        $definitions = [];

        foreach ($table->columns as $column) {
            $definitions[] = $this->columnToFlow($column, $table);
        }

        return new Schema(...$definitions);
    }

    public function toPostgreSqlTable(
        Schema $schema,
        string $tableName,
        string $databaseSchema = 'public',
        ?TableOptions $options = null,
    ): Table {
        $columns = [];
        $position = 1;

        foreach ($schema->definitions() as $definition) {
            $columns[] = $this->flowToColumn($definition, $position);
            $position++;
        }

        if ($columns === []) {
            throw new RuntimeException(
                'Cannot convert an empty Flow schema to a PostgreSQL table: at least one column is required.',
            );
        }

        return (new Table(
            schema: $databaseSchema,
            name: $tableName,
            columns: $columns,
            primaryKey: $this->primaryKey($schema),
            indexes: $this->indexes($schema),
            uniqueConstraints: $this->uniqueConstraints($schema),
        ))->withOptions($options ?? new TableOptions());
    }

    /**
     * @return Definition<mixed>
     */
    private function columnToFlow(Column $column, Table $table): Definition
    {
        $nullable = $column->nullable;
        $metadata = Metadata::empty();

        $typeShape = $column->type->normalize();
        $precision = $typeShape['precision'] ?? null;
        $scale = $typeShape['scale'] ?? null;

        if (($typeShape['name'] === 'varchar' || $typeShape['name'] === 'bpchar') && $precision !== null) {
            $metadata = $metadata->merge(PostgreSqlMetadata::length($precision));
        }

        if ($typeShape['name'] === 'numeric') {
            if ($precision !== null) {
                $metadata = $metadata->merge(PostgreSqlMetadata::precision($precision));
            }

            if ($scale !== null) {
                $metadata = $metadata->merge(PostgreSqlMetadata::scale($scale));
            }
        }

        if ($column->isIdentity) {
            $metadata = $metadata->merge(PostgreSqlMetadata::identity(
                $column->identityGeneration ?? IdentityGeneration::ALWAYS,
            ));
        }

        if ($column->isGenerated && $column->generationExpression !== null) {
            $metadata = $metadata->merge(PostgreSqlMetadata::generated($column->generationExpression));
        }

        if ($table->primaryKey !== null && in_array($column->name, $table->primaryKey->columns, true)) {
            $metadata = $metadata->merge(PostgreSqlMetadata::primaryKey($table->primaryKey->name ?? ''));
            $nullable = false;
        }

        foreach ($table->uniqueConstraints as $unique) {
            $position = array_search($column->name, $unique->columns, true);

            if ($position !== false) {
                $metadata = $metadata->merge(PostgreSqlMetadata::indexUnique($unique->name ?? '', $position));
            }
        }

        foreach ($table->indexes as $index) {
            if ($index->unique || $index->primary) {
                continue;
            }

            $position = array_search($column->name, $index->columns, true);

            if ($position !== false) {
                $metadata = $metadata->merge(PostgreSqlMetadata::index($index->name, $position));
            }
        }

        return definition_from_type($column->name, $this->typesMap->toFlowType($column->type), $nullable, $metadata);
    }

    /**
     * @param Definition<mixed> $definition
     */
    private function flowToColumn(Definition $definition, int $position): Column
    {
        $metadata = $definition->metadata();

        $default = null;

        if ($metadata->has(PostgreSqlMetadata::DEFAULT->value)) {
            /** @var bool|float|int|string $default */
            $default = $metadata->get(PostgreSqlMetadata::DEFAULT->value);
        }

        $isIdentity = $metadata->has(PostgreSqlMetadata::IDENTITY->value);
        $identityGeneration = $isIdentity
            ? IdentityGeneration::from((string) $metadata->getAs(PostgreSqlMetadata::IDENTITY->value, type_string()))
            : null;

        $generationExpression = $metadata->has(PostgreSqlMetadata::GENERATED->value)
            ? (string) $metadata->getAs(PostgreSqlMetadata::GENERATED->value, type_string())
            : null;

        return Column::create(
            name: $definition->entry()->name(),
            type: $this->columnType($definition->type(), $metadata),
            nullable: $definition->isNullable() && !$metadata->has(PostgreSqlMetadata::PRIMARY_KEY->value),
            default: $default,
            isIdentity: $isIdentity,
            identityGeneration: $identityGeneration,
            isGenerated: $generationExpression !== null,
            generationExpression: $generationExpression,
            ordinalPosition: $position,
        );
    }

    /**
     * @param Type<mixed> $type
     */
    private function columnType(Type $type, Metadata $metadata): ColumnType
    {
        if ($metadata->has(PostgreSqlMetadata::TYPE->value)) {
            return column_type_from_string((string) $metadata->getAs(PostgreSqlMetadata::TYPE->value, type_string()));
        }

        if ($metadata->has(PostgreSqlMetadata::LENGTH->value)) {
            return ColumnType::varchar((int) $metadata->getAs(PostgreSqlMetadata::LENGTH->value, type_integer()));
        }

        if ($metadata->has(PostgreSqlMetadata::PRECISION->value)) {
            return ColumnType::numeric(
                (int) $metadata->getAs(PostgreSqlMetadata::PRECISION->value, type_integer()),
                $metadata->has(PostgreSqlMetadata::SCALE->value)
                    ? (int) $metadata->getAs(PostgreSqlMetadata::SCALE->value, type_integer())
                    : null,
            );
        }

        return $this->typesMap->toColumnType($type);
    }

    /**
     * @return array<string, non-empty-list<string>>
     */
    private function groupColumnsByMetadata(Schema $schema, string $key): array
    {
        $grouped = [];

        foreach ($schema->definitions() as $definition) {
            if ($definition->metadata()->has($key)) {
                $name = (string) $definition->metadata()->getAs($key, type_string());
                $grouped[$name][] = $definition->entry()->name();
            }
        }

        return $grouped;
    }

    /**
     *
     * @return array<string, non-empty-list<string>>
     */
    private function groupColumnsByIndexPrefix(Schema $schema, string $prefix): array
    {
        $prefix .= ':';
        /** @var array<string, list<array{column: string, position: int, ordinal: int}>> $grouped */
        $grouped = [];
        $ordinal = 0;

        foreach ($schema->definitions() as $definition) {
            foreach ($definition->metadata()->normalize() as $key => $value) {
                if (str_starts_with($key, $prefix)) {
                    $grouped[substr($key, strlen($prefix))][] = [
                        'column' => $definition->entry()->name(),
                        'position' => is_int($value) ? $value : PHP_INT_MAX,
                        'ordinal' => $ordinal,
                    ];
                }
            }

            $ordinal++;
        }

        $ordered = [];

        foreach ($grouped as $name => $columns) {
            if ($columns === []) {
                continue;
            }

            usort(
                $columns,
                static fn(array $a, array $b): int => (
                    [$a['position'], $a['ordinal']] <=> [$b['position'], $b['ordinal']]
                ),
            );
            $ordered[$name] = array_map(static fn(array $c): string => $c['column'], $columns);
        }

        return $ordered;
    }

    /**
     * @return list<Index>
     */
    private function indexes(Schema $schema): array
    {
        $indexes = [];

        foreach ($this->groupColumnsByIndexPrefix($schema, PostgreSqlMetadata::INDEX->value) as $name => $columns) {
            $indexes[] = new Index(name: $name, columns: $columns);
        }

        return $indexes;
    }

    private function primaryKey(Schema $schema): ?PrimaryKey
    {
        $grouped = $this->groupColumnsByMetadata($schema, PostgreSqlMetadata::PRIMARY_KEY->value);

        if ($grouped === []) {
            return null;
        }

        if (count($grouped) > 1) {
            throw new RuntimeException(
                'Each table can have only one primary key, provided: ' . implode(', ', array_keys($grouped)),
            );
        }

        $name = array_keys($grouped)[0];

        return new PrimaryKey(columns: $grouped[$name], name: $name === '' ? null : $name);
    }

    /**
     * @return list<UniqueConstraint>
     */
    private function uniqueConstraints(Schema $schema): array
    {
        $constraints = [];

        foreach ($this->groupColumnsByIndexPrefix(
            $schema,
            PostgreSqlMetadata::INDEX_UNIQUE->value,
        ) as $name => $columns) {
            $constraints[] = new UniqueConstraint(columns: $columns, name: $name === '' ? null : $name);
        }

        return $constraints;
    }
}
