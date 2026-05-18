<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function array_keys;
use function array_map;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class SchemaConverter
{
    /**
     * @return Schema
     */
    public function toFlow(ParquetSchema $schema): Schema
    {
        return schema(...array_map(fn(Column $parquetColumn) => $this->parquetToFlowDefinition(
            $parquetColumn,
        ), $schema->columns()));
    }

    /**
     * @param Schema $schema
     */
    public function toParquet(Schema $schema): ParquetSchema
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $columns[] = $this->flowToParquet(
                $definition->entry()->name(),
                $definition->type(),
                $definition->isNullable(),
            );
        }

        return ParquetSchema::with(...$columns);
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowToParquet(string $name, Type $type, bool $nullable): Column
    {
        switch ($type::class) {
            case FloatType::class:
                return FlatColumn::float(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case IntegerType::class:
                return FlatColumn::int64(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case HTMLType::class:
            case HTMLElementType::class:
            case XMLElementType::class:
            case XMLType::class:
            case StringType::class:
                return FlatColumn::string(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case BooleanType::class:
                return FlatColumn::boolean(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case TimeType::class:
                return FlatColumn::time(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case DateType::class:
                return FlatColumn::date(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case DateTimeType::class:
                return FlatColumn::datetime(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case UuidType::class:
                return FlatColumn::uuid(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case JsonType::class:
                return FlatColumn::json(
                    $name,
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case ListType::class:
                $elementType = $type->element();
                $elementOptional = $elementType instanceof OptionalType;
                $elementType = $elementType instanceof OptionalType ? $elementType->base() : $elementType;

                return NestedColumn::list(
                    $name,
                    new ListElement($this->flowToParquet('element', $elementType, $elementOptional)),
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case MapType::class:
                $valueType = $type->value();
                $valueOptional = $valueType instanceof OptionalType;
                $valueType = $valueType instanceof OptionalType ? $valueType->base() : $valueType;

                return NestedColumn::map(
                    $name,
                    new ParquetSchema\MapKey($this->flowToParquet('key', $type->key(), false)),
                    new ParquetSchema\MapValue($this->flowToParquet('value', $valueType, $valueOptional)),
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
            case StructureType::class:
                return NestedColumn::struct(
                    $name,
                    array_map(
                        function (string $elementName, Type $elementType) {
                            $elementOptional = $elementType instanceof OptionalType;
                            $elementType = $elementType instanceof OptionalType ? $elementType->base() : $elementType;

                            return $this->flowToParquet($elementName, $elementType, $elementOptional);
                        },
                        array_keys($type->elements()),
                        $type->elements(),
                    ),
                    $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED,
                );
        }

        throw new RuntimeException($type::class . ' is not supported.');
    }

    /**
     * @return Definition<mixed>
     */
    private function parquetToFlowDefinition(Column $column): Definition
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
                type_list($this->parquetToFlowType($column->getListElement())),
                $nullable,
            );
        }

        if ($column->isMap()) {
            $keyType = $this->parquetToFlowType($column->getMapKeyColumn());

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            $mapValueColumn = $column->getMapValueColumn();
            $valueType = $mapValueColumn !== null
                ? $this->parquetToFlowType($mapValueColumn)
                : type_optional(type_string());

            return map_schema($column->name(), type_map($keyType, $valueType), $nullable);
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->parquetToFlowType($structColumn);
        }

        return structure_schema($column->name(), type_structure($elements), $nullable);
    }

    /**
     * @return Type<mixed>
     */
    private function parquetToFlowType(Column $column): Type
    {
        if ($column instanceof FlatColumn) {
            $logicalType = $column->logicalType();

            $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

            if ($logicalType === null) {
                $type = match ($column->type()) {
                    ParquetSchema\PhysicalType::INT32 => match ($column->convertedType()) {
                        ParquetSchema\ConvertedType::DATE => type_date(),
                        default => type_integer(),
                    },
                    ParquetSchema\PhysicalType::INT64 => type_integer(),
                    ParquetSchema\PhysicalType::BOOLEAN => type_boolean(),
                    ParquetSchema\PhysicalType::DOUBLE => type_float(),
                    ParquetSchema\PhysicalType::FLOAT => type_float(),
                    ParquetSchema\PhysicalType::BYTE_ARRAY => type_string(),
                    default => throw new RuntimeException($column->type()->name . ' is not supported.'),
                };

                return $nullable ? type_optional($type) : $type;
            }

            $type = match ($logicalType->name()) {
                ParquetSchema\LogicalType::STRING => type_string(),
                ParquetSchema\LogicalType::TIME => type_time(),
                ParquetSchema\LogicalType::DATE => type_date(),
                ParquetSchema\LogicalType::TIMESTAMP => type_datetime(),
                ParquetSchema\LogicalType::UUID => type_uuid(),
                ParquetSchema\LogicalType::JSON => type_json(),
                ParquetSchema\LogicalType::DECIMAL => type_float(),
                ParquetSchema\LogicalType::INTEGER => type_integer(),
                default => throw new RuntimeException($logicalType->name() . ' is not supported.'),
            };

            return $nullable ? type_optional($type) : $type;
        }

        /** @var NestedColumn $column */
        $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

        if ($column->isList()) {
            return $nullable
                ? type_optional(type_list($this->parquetToFlowType($column->getListElement())))
                : type_list($this->parquetToFlowType($column->getListElement()));
        }

        if ($column->isMap()) {
            $keyType = $this->parquetToFlowType($column->getMapKeyColumn());

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            $mapValueColumn = $column->getMapValueColumn();
            $valueType = $mapValueColumn !== null
                ? $this->parquetToFlowType($mapValueColumn)
                : type_optional(type_string());

            return $nullable ? type_optional(type_map($keyType, $valueType)) : type_map($keyType, $valueType);
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->parquetToFlowType($structColumn);
        }

        return $nullable ? type_optional(type_structure($elements)) : type_structure($elements);
    }
}
