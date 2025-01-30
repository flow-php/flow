<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\type_string;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type as DbalType};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, DateType, JsonType, ListType, MapType, StructureType, TimeType, UuidType, XMLElementType, XMLType};
use Flow\ETL\PHP\Type\Native\{BooleanType, FloatType, IntegerType, StringType};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema;

final class SchemaConverter
{
    public const DEFAULT_TYPES = [
        StringType::class => \Doctrine\DBAL\Types\StringType::class,
        IntegerType::class => \Doctrine\DBAL\Types\IntegerType::class,
        FloatType::class => \Doctrine\DBAL\Types\FloatType::class,
        BooleanType::class => \Doctrine\DBAL\Types\BooleanType::class,
        DateType::class => \Doctrine\DBAL\Types\DateImmutableType::class,
        TimeType::class => \Doctrine\DBAL\Types\TimeImmutableType::class,
        DateTimeType::class => \Doctrine\DBAL\Types\DateTimeImmutableType::class,
        UuidType::class => \Doctrine\DBAL\Types\GuidType::class,
        JsonType::class => \Doctrine\DBAL\Types\JsonType::class,
        XMLType::class => \Doctrine\DBAL\Types\StringType::class,
        XMLElementType::class => \Doctrine\DBAL\Types\StringType::class,
        ListType::class => \Doctrine\DBAL\Types\JsonType::class,
        MapType::class => \Doctrine\DBAL\Types\JsonType::class,
        StructureType::class => \Doctrine\DBAL\Types\JsonType::class,
    ];

    /**
     * @var array<class-string<Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>>
     */
    private array $typesMap;

    /**
     * @param array<class-string<Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $typesMap
     */
    public function __construct(array $typesMap = [])
    {
        foreach ($typesMap as $flowType => $dbalType) {
            if (!\is_a($flowType, Type::class, true)) {
                throw new InvalidArgumentException(\sprintf('"%s" is not a valid type.', $flowType));
            }

            if (!\is_a($dbalType, DbalType::class, true)) {
                throw new InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType::class));
            }
        }

        if (!\count($typesMap)) {
            $this->typesMap = self::DEFAULT_TYPES;
        } else {
            $this->typesMap = $typesMap;
        }
    }

    public function toDbalTable(Schema $schema, string $tableName, array $tableOptions = []) : Table
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $column = $this->flowToColumn($definition->entry()->name(), $definition->type(), $definition->metadata());
            $columns[$column->getName()] = $column;
        }

        $table = new Table($tableName, $columns, options: $tableOptions);
        $this->updateIndexes($schema, $table);

        return $table;
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowToColumn(string $name, Type $type, ?Schema\Metadata $metadata = null) : Column
    {
        if (!\array_key_exists($type::class, $this->typesMap)) {
            throw new InvalidArgumentException(\sprintf('"%s" is not a valid type.', $type::class));
        }

        $dbalTypeClass = $this->typesMap[$type::class] ?? null;

        if ($dbalTypeClass === null) {
            throw new InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $type::class));
        }

        $dbalType = null;

        foreach (DbalType::getTypesMap() as $typeName => $class) {
            if ($class === $dbalTypeClass) {
                $dbalType = DbalType::getType($typeName);

                break;
            }
        }

        if ($dbalType === null) {
            throw new InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $type::class));
        }

        $options = [
            'notnull' => !$type->nullable(),
        ];

        if ($type instanceof FloatType) {
            // with decimals precision and scale are confusing, in float precision is number of digits, not digits before/after decimal point
            // with decimals precision is total number of digits, and scale is number of digits after decimal point
            $options['scale'] = $type->precision;
        }

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

    private function updateIndexes(Schema $schema, Table $table) : array
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
                $uniqueIndex = (string) $definition->metadata()->getAs(DbalMetadata::INDEX_UNIQUE->value, type_string());

                if (!\array_key_exists($uniqueIndex, $uniqueIndexesData)) {
                    $uniqueIndexesData[$uniqueIndex] = [];
                }

                $uniqueIndexesData[$uniqueIndex][] = $definition->entry()->name();
            }

            if ($definition->metadata()->has(DbalMetadata::PRIMARY_KEY->value)) {
                $primaryKeyName = (string) $definition->metadata()->getAs(DbalMetadata::PRIMARY_KEY->value, type_string());
                $primaryKey[$primaryKeyName][] = $definition->entry()->name();

                if (\count($primaryKey) > 1) {
                    throw new InvalidArgumentException('Each table can have only one primary key, provided: ' . \implode(', ', \array_keys($primaryKey)));
                }
            }
        }

        $indexes = [];

        foreach ($indexesData as $name => $columns) {
            $table->addIndex($columns, $name);
        }

        foreach ($uniqueIndexesData as $name => $columns) {
            $indexes[] = new \Doctrine\DBAL\Schema\Index($name, $columns, isUnique: true);
            $table->addUniqueIndex($columns, $name);
        }

        foreach ($primaryKey as $name => $columns) {
            $table->setPrimaryKey($columns, $name);
        }

        return $indexes;
    }
}
