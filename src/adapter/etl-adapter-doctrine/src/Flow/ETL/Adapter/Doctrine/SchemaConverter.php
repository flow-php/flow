<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type as DbalType;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_string;

final readonly class SchemaConverter
{
    private TypesMap $typesMap;

    /**
     * @param array<class-string<\Flow\Types\Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $map
     */
    public function __construct(array $map = [])
    {
        $this->typesMap = new TypesMap($map);
    }

    /**
     * @param array<array-key, mixed> $tableOptions
     */
    public function toDbalTable(Schema $schema, string $tableName, array $tableOptions = []): Table
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $column = $this->flowToColumn(
                $definition->entry()->name(),
                $definition->type(),
                $definition->isNullable(),
                $definition->metadata(),
            );
            $columns[$column->getName()] = $column;
        }

        $table = new Table($tableName, $columns, options: $tableOptions);
        $this->updateIndexes($schema, $table);

        return $table;
    }

    public function toFlowSchema(Table $table): Schema
    {
        $definitions = [];

        foreach ($table->getColumns() as $column) {
            $definitions[] = $this->columnToFlow($column, $table);
        }

        return new Schema(...$definitions);
    }

    /**
     * @return Definition<mixed>
     */
    private function columnToFlow(Column $column, Table $table): Definition
    {
        $type = $this->typesMap->toFlowType($column->getType()::class);

        $nullable = !$column->getNotnull();

        $metadata = Metadata::empty();

        if ($column->getLength() !== null) {
            $metadata = $metadata->merge(DbalMetadata::length($column->getLength()));
        }

        if ($column->getDefault() !== null) {
            $defaultValue = $column->getDefault();

            if (\is_scalar($defaultValue)) {
                $metadata = $metadata->merge(DbalMetadata::default($defaultValue));
            }
        }

        if ($column->getPrecision() !== null) {
            $metadata = $metadata->merge(DbalMetadata::precision($column->getPrecision()));
        }

        if ($column->getScale() !== 0) {
            $metadata = $metadata->merge(DbalMetadata::scale($column->getScale()));
        }

        if ($column->getPlatformOptions() !== []) {
            $metadata = $metadata->merge(DbalMetadata::platformOptions($column->getPlatformOptions()));
        }

        if ($column->getColumnDefinition() !== null) {
            $metadata = $metadata->merge(DbalMetadata::columnDefinition($column->getColumnDefinition()));
        }

        if ($column->getUnsigned() !== false) {
            $metadata = $metadata->merge(DbalMetadata::unsigned($column->getUnsigned()));
        }

        if ($column->getFixed() !== false) {
            $metadata = $metadata->merge(DbalMetadata::fixed($column->getFixed()));
        }

        /** @phpstan-ignore-next-line */
        if ($column->getComment() && $column->getComment() !== '') {
            $metadata = $metadata->merge(DbalMetadata::comment($column->getComment()));
        }

        foreach ($table->getPrimaryKey()?->getColumns() ?? [] as $primaryKeyColumn) {
            if ($primaryKeyColumn === $column->getName()) {
                $metadata = $metadata->merge(DbalMetadata::primaryKey($table->getPrimaryKey()?->getName() ?? ''));
                $nullable = false;
            }
        }

        foreach ($table->getIndexes() as $index) {
            if (
                $index->isUnique()
                && !$index->isPrimary()
                && \in_array($column->getName(), $index->getColumns(), true)
            ) {
                $metadata = $metadata->merge(DbalMetadata::indexUnique($index->getName()));
            }

            if (
                !$index->isUnique()
                && !$index->isPrimary()
                && \in_array($column->getName(), $index->getColumns(), true)
            ) {
                $metadata = $metadata->merge(DbalMetadata::index($index->getName()));
            }
        }

        return definition_from_type($column->getName(), $type, $nullable, $metadata);
    }

    /**
     * @param \Flow\Types\Type<mixed> $type
     */
    private function flowToColumn(string $name, Type $type, bool $nullable, ?Metadata $metadata = null): Column
    {
        $dbalTypeClass = $this->typesMap->toDbalType($type::class);

        if ($metadata?->has(DbalMetadata::TYPE->value)) {
            $dbalType = DbalType::getType((string) $metadata->getAs(DbalMetadata::TYPE->value, type_string()));
        } else {
            $dbalType = null;

            foreach (DbalType::getTypesMap() as $typeName => $class) {
                if ($class === $dbalTypeClass) {
                    $dbalType = DbalType::getType($typeName);

                    break;
                }
            }
        }

        if ($dbalType === null) {
            throw new InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType));
        }

        $options = [
            'notnull' => !$nullable,
        ];

        if ($metadata?->has(DbalMetadata::LENGTH->value)) {
            $options['length'] = $metadata->get(DbalMetadata::LENGTH->value);
        }

        if ($metadata?->has(DbalMetadata::DEFAULT->value)) {
            $options['default'] = $metadata->get(DbalMetadata::DEFAULT->value);
        }

        if ($metadata?->has(DbalMetadata::PRECISION->value)) {
            $options['precision'] = $metadata->get(DbalMetadata::PRECISION->value);
        }

        if ($metadata?->has(DbalMetadata::SCALE->value)) {
            $options['scale'] = $metadata->get(DbalMetadata::SCALE->value);
        }

        if ($metadata?->has(DbalMetadata::PLATFORM_OPTIONS->value)) {
            $options['platformOptions'] = $metadata->get(DbalMetadata::PLATFORM_OPTIONS->value);
        }

        if ($metadata?->has(DbalMetadata::COLUMN_DEFINITION->value)) {
            $options['columnDefinition'] = $metadata->get(DbalMetadata::COLUMN_DEFINITION->value);
        }

        if ($metadata?->has(DbalMetadata::UNSIGNED->value)) {
            $options['unsigned'] = $metadata->get(DbalMetadata::UNSIGNED->value);
        }

        if ($metadata?->has(DbalMetadata::FIXED->value)) {
            $options['fixed'] = $metadata->get(DbalMetadata::FIXED->value);
        }

        if ($metadata?->has(DbalMetadata::COMMENT->value)) {
            $options['comment'] = $metadata->get(DbalMetadata::COMMENT->value);
        }

        if ($metadata?->has(DbalMetadata::CUSTOM_SCHEMA_OPTIONS->value)) {
            $options['customSchemaOptions'] = $metadata->get(DbalMetadata::CUSTOM_SCHEMA_OPTIONS->value);
        }

        return new Column($name, $dbalType, $options);
    }

    /**
     * @return array<Index>
     */
    private function updateIndexes(Schema $schema, Table $table): array
    {
        $indexesData = [];
        $uniqueIndexesData = [];
        $primaryKey = [];

        foreach ($schema->definitions() as $definition) {
            if ($definition->metadata()->has(DbalMetadata::INDEX->value)) {
                $index = (string) $definition->metadata()->getAs(DbalMetadata::INDEX->value, type_string());

                if (!\array_key_exists($index, $indexesData)) {
                    $indexesData[$index] = [];
                }

                $indexesData[$index][] = $definition->entry()->name();
            }

            if ($definition->metadata()->has(DbalMetadata::INDEX_UNIQUE->value)) {
                $uniqueIndex = (string) $definition->metadata()->getAs(
                    DbalMetadata::INDEX_UNIQUE->value,
                    type_string(),
                );

                if (!\array_key_exists($uniqueIndex, $uniqueIndexesData)) {
                    $uniqueIndexesData[$uniqueIndex] = [];
                }

                $uniqueIndexesData[$uniqueIndex][] = $definition->entry()->name();
            }

            if ($definition->metadata()->has(DbalMetadata::PRIMARY_KEY->value)) {
                $primaryKeyName = (string) $definition->metadata()->getAs(
                    DbalMetadata::PRIMARY_KEY->value,
                    type_string(),
                );
                $primaryKey[$primaryKeyName][] = $definition->entry()->name();

                if (\count($primaryKey) > 1) {
                    throw new InvalidArgumentException(
                        'Each table can have only one primary key, provided: '
                            . \implode(', ', \array_keys($primaryKey)),
                    );
                }
            }
        }

        $indexes = [];

        foreach ($indexesData as $name => $columns) {
            $table->addIndex($columns, $name);
        }

        foreach ($uniqueIndexesData as $name => $columns) {
            $indexes[] = new Index($name, $columns, isUnique: true);
            $table->addUniqueIndex($columns, $name);
        }

        foreach ($primaryKey as $name => $columns) {
            $table->setPrimaryKey($columns, $name);
        }

        return $indexes;
    }
}
