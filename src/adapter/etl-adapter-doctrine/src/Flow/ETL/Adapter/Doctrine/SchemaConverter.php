<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Index\IndexedColumn;
use Doctrine\DBAL\Schema\Index\IndexType;
use Doctrine\DBAL\Schema\Name\UnqualifiedName;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type as DbalType;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function array_key_exists;
use function array_keys;
use function array_map;
use function array_slice;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_string;
use function implode;
use function in_array;
use function is_scalar;
use function sprintf;

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
     * @param array<string, mixed> $tableOptions
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
            $columns[$column->getObjectName()->getIdentifier()->getValue()] = $column;
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

        $length = $column->getLength();

        if ($length !== null) {
            $metadata = $metadata->merge(DbalMetadata::length($length));
        }

        // @mago-expect analysis:mixed-assignment
        $defaultValue = $column->getDefault();

        if ($defaultValue !== null && is_scalar($defaultValue)) {
            $metadata = $metadata->merge(DbalMetadata::default($defaultValue));
        }

        $precision = $column->getPrecision();

        if ($precision !== null) {
            $metadata = $metadata->merge(DbalMetadata::precision($precision));
        }

        if ($column->getScale() !== 0) {
            $metadata = $metadata->merge(DbalMetadata::scale($column->getScale()));
        }

        // @mago-expect analysis:deprecated-method
        if ($column->getPlatformOptions() !== []) {
            // @mago-expect analysis:deprecated-method
            $metadata = $metadata->merge(DbalMetadata::platformOptions($column->getPlatformOptions()));
        }

        $columnDefinition = $column->getColumnDefinition();

        if ($columnDefinition !== null) {
            $metadata = $metadata->merge(DbalMetadata::columnDefinition($columnDefinition));
        }

        if ($column->getUnsigned() !== false) {
            $metadata = $metadata->merge(DbalMetadata::unsigned($column->getUnsigned()));
        }

        if ($column->getFixed() !== false) {
            $metadata = $metadata->merge(DbalMetadata::fixed($column->getFixed()));
        }

        $comment = $column->getComment();

        if ($comment !== '') {
            $metadata = $metadata->merge(DbalMetadata::comment($comment));
        }

        $primaryKeyConstraint = $table->getPrimaryKeyConstraint();
        $pkColumnNames = array_map(
            static fn(UnqualifiedName $n): string => $n->getIdentifier()->getValue(),
            $primaryKeyConstraint?->getColumnNames() ?? [],
        );
        $columnName = $column->getObjectName()->getIdentifier()->getValue();

        foreach ($pkColumnNames as $primaryKeyColumn) {
            if ($primaryKeyColumn === $columnName) {
                $metadata = $metadata->merge(DbalMetadata::primaryKey(
                    $primaryKeyConstraint?->getObjectName()?->getIdentifier()->getValue() ?? '',
                ));
                $nullable = false;
            }
        }

        foreach ($table->getIndexes() as $index) {
            $indexColumnNames = array_map(static fn(IndexedColumn $c): string => $c
                ->getColumnName()
                ->getIdentifier()
                ->getValue(), $index->getIndexedColumns());

            if (
                $index->getType() === IndexType::UNIQUE
                && !in_array($columnName, $pkColumnNames, true)
                && in_array($columnName, $indexColumnNames, true)
            ) {
                $indexName = $index->getObjectName();
                $metadata = $metadata->merge(DbalMetadata::indexUnique($indexName->getIdentifier()->getValue()));
            }

            if (
                $index->getType() === IndexType::REGULAR
                && !in_array($columnName, $pkColumnNames, true)
                && in_array($columnName, $indexColumnNames, true)
            ) {
                $indexName = $index->getObjectName();
                $metadata = $metadata->merge(DbalMetadata::index($indexName->getIdentifier()->getValue()));
            }
        }

        return definition_from_type($columnName, $type, $nullable, $metadata);
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
            throw new InvalidArgumentException(sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType));
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

                if (!array_key_exists($index, $indexesData)) {
                    $indexesData[$index] = [];
                }

                $indexesData[$index][] = $definition->entry()->name();
            }

            if ($definition->metadata()->has(DbalMetadata::INDEX_UNIQUE->value)) {
                $uniqueIndex = (string) $definition->metadata()->getAs(
                    DbalMetadata::INDEX_UNIQUE->value,
                    type_string(),
                );

                if (!array_key_exists($uniqueIndex, $uniqueIndexesData)) {
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

                if (count($primaryKey) > 1) {
                    throw new InvalidArgumentException(
                        'Each table can have only one primary key, provided: ' . implode(', ', array_keys($primaryKey)),
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
            /** @var non-empty-list<non-empty-string> $columns */
            $editor = PrimaryKeyConstraint::editor()->setUnquotedColumnNames($columns[0], ...array_slice($columns, 1));

            if ($name !== '') {
                $editor = $editor->setUnquotedName($name);
            }

            $table->addPrimaryKeyConstraint($editor->create());
        }

        return $indexes;
    }
}
