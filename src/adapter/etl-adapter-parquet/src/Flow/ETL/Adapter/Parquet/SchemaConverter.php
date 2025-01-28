<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\{bool_schema,
    date_schema,
    datetime_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    str_schema,
    struct_schema,
    time_schema,
    type_list,
    type_map,
    type_structure,
    uuid_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\{DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{BooleanType, FloatType, IntegerType, StringType};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\{Schema};
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, ListElement, NestedColumn};

final class SchemaConverter
{
    public function toFlow(ParquetSchema $schema) : Schema
    {
        return \Flow\ETL\DSL\schema(...\array_map(
            fn (Column $parquetColumn) => $this->parquetToFlow($parquetColumn),
            $schema->columns()
        ));
    }

    public function toParquet(Schema $schema) : ParquetSchema
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $columns[] = $this->flowToParquet(
                $definition->entry()->name(),
                $definition->type()
            );
        }

        return ParquetSchema::with(...$columns);
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowToParquet(string $name, Type $type) : Column
    {
        switch ($type::class) {
            case FloatType::class:
                return FlatColumn::float($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case IntegerType::class:
                return FlatColumn::int64($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case StringType::class:
                return FlatColumn::string($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case BooleanType::class:
                return FlatColumn::boolean($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case TimeType::class:
                return FlatColumn::time($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case DateType::class:
                return FlatColumn::date($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case DateTimeType::class:
                return FlatColumn::datetime($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case UuidType::class:
                return FlatColumn::uuid($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case JsonType::class:
                return FlatColumn::json($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case XMLType::class:
            case XMLElementType::class:
                return FlatColumn::string($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case ListType::class:
                return NestedColumn::list($name, new ListElement($this->flowToParquet('element', $type->element())), $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case MapType::class:
                return NestedColumn::map(
                    $name,
                    new ParquetSchema\MapKey($this->flowToParquet('key', $type->key())),
                    new ParquetSchema\MapValue($this->flowToParquet('value', $type->value())),
                    $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED
                );
            case StructureType::class:
                return NestedColumn::struct(
                    $name,
                    \array_map(
                        fn (string $elementName, Type $elementType) => $this->flowToParquet($elementName, $elementType),
                        \array_keys($type->elements()),
                        $type->elements()
                    ),
                    $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED
                );
        }

        throw new RuntimeException($type::class . ' is not supported.');
    }

    private function parquetToFlow(Column $column) : Schema\Definition
    {
        if ($column instanceof FlatColumn) {
            $logicalType = $column->logicalType();

            $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

            if ($logicalType === null) {
                return match ($column->type()) {
                    ParquetSchema\PhysicalType::INT32 => match ($column->convertedType()) {
                        ParquetSchema\ConvertedType::DATE => date_schema($column->name(), $nullable),
                        default => int_schema($column->name(), $nullable),
                    },
                    ParquetSchema\PhysicalType::INT64 => int_schema($column->name(), $nullable),
                    ParquetSchema\PhysicalType::BOOLEAN => bool_schema($column->name(), $nullable),
                    ParquetSchema\PhysicalType::DOUBLE => float_schema($column->name(), $nullable),
                    ParquetSchema\PhysicalType::FLOAT => float_schema($column->name(), $nullable),
                    ParquetSchema\PhysicalType::BYTE_ARRAY => str_schema($column->name(), $nullable),
                    default => throw new RuntimeException($column->type()->name . ' is not supported.'),
                };
            }

            return match ($logicalType->name()) {
                ParquetSchema\LogicalType::STRING => str_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::TIME => time_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::DATE => date_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::TIMESTAMP => datetime_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::UUID => uuid_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::JSON => json_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::DECIMAL => float_schema($column->name(), $nullable),
                ParquetSchema\LogicalType::INTEGER => int_schema($column->name(), $nullable),
                default => throw new RuntimeException($logicalType->name() . ' is not supported.'),
            };
        }

        /** @var NestedColumn $column */
        $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

        if ($column->isList()) {
            return list_schema(
                $column->name(),
                type_list(
                    $this->parquetToFlow($column->getListElement())->type(),
                    $nullable
                )
            );
        }

        if ($column->isMap()) {
            $keyType = $this->parquetToFlow($column->getMapKeyColumn())->type();

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            return map_schema(
                $column->name(),
                type_map(
                    $keyType,
                    $this->parquetToFlow($column->getMapValueColumn())->type(),
                    $nullable
                )
            );
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->parquetToFlow($structColumn)->type();
        }

        return struct_schema($column->name(), type_structure($elements, $nullable));
    }
}
