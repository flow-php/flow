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
use Flow\ETL\PHP\Type\Native\{BooleanType, FloatType, IntegerType, ObjectType, StringType};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Row\{Schema};
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, ListElement, NestedColumn};

final class SchemaConverter
{
    public function fromParquet(ParquetSchema $schema) : Schema
    {
        $definitions = [];

        foreach ($schema->columns() as $column) {
            $definitions[] = $this->fromParquetColumnToFlowDefinition($column);
        }

        return \Flow\ETL\DSL\schema(...$definitions);
    }

    public function toParquet(Schema $schema) : ParquetSchema
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $columns[] = $this->flowTypeToParquetType(
                $definition->entry()->name(),
                $definition->type()
            );
        }

        return ParquetSchema::with(...$columns);
    }

    /**
     * @param Type<mixed> $elementType
     */
    private function flowListToParquetList(Type $elementType) : ListElement
    {
        switch ($elementType::class) {
            case FloatType::class:
                return ListElement::float(!$elementType->nullable());
            case IntegerType::class:
                return ListElement::int64(!$elementType->nullable());
            case StringType::class:
                return ListElement::string(!$elementType->nullable());
            case BooleanType::class:
                return ListElement::boolean(!$elementType->nullable());
            case DateTimeType::class:
                return ListElement::datetime(!$elementType->nullable());
            case DateType::class:
                return ListElement::date(!$elementType->nullable());
            case TimeType::class:
                return ListElement::time(!$elementType->nullable());
            case UuidType::class:
                return ListElement::uuid(!$elementType->nullable());
            case JsonType::class:
                return ListElement::json(!$elementType->nullable());
            case XMLType::class:
            case XMLElementType::class:
                return ListElement::string(!$elementType->nullable());
            case ObjectType::class:
                $class = $elementType->class;

                if ($class === \DateInterval::class) {
                    return ListElement::time(!$elementType->nullable());
                }

                throw new \Flow\Parquet\Exception\RuntimeException($class . ' can\'t be converted to any parquet columns.');
            case ListType::class:
                return ListElement::list($this->flowListToParquetList($elementType->element()), !$elementType->nullable());
            case MapType::class:
                return ListElement::map(
                    $this->flowMapKeyToParquetMapKey($elementType->key()),
                    $this->flowMapValueToParquetMapValue($elementType->value()),
                    !$elementType->nullable()
                );
            case StructureType::class:
                return ListElement::structure($this->flowStructureToParquetStructureElements($elementType), !$elementType->nullable());
        }

        throw new RuntimeException($elementType::class . ' is not supported.');
    }

    /**
     * @param Type<mixed> $mapKeyType
     */
    private function flowMapKeyToParquetMapKey(Type $mapKeyType) : ParquetSchema\MapKey
    {
        switch ($mapKeyType::class) {
            case UuidType::class:
                return ParquetSchema\MapKey::uuid();
            case DateTimeType::class:
                return ParquetSchema\MapKey::datetime();
            case DateType::class:
                return ParquetSchema\MapKey::date();
            case TimeType::class:
                return ParquetSchema\MapKey::time();
            case FloatType::class:
                return ParquetSchema\MapKey::float();
            case IntegerType::class:
                return ParquetSchema\MapKey::int64();
            case StringType::class:
                return ParquetSchema\MapKey::string();
            case BooleanType::class:
                return ParquetSchema\MapKey::boolean();
        }

        throw new RuntimeException($mapKeyType::class . ' is not supported.');
    }

    /**
     * @param Type<mixed> $mapValueType
     */
    private function flowMapValueToParquetMapValue(Type $mapValueType) : ParquetSchema\MapValue
    {
        switch ($mapValueType::class) {
            case FloatType::class:
                return ParquetSchema\MapValue::float(!$mapValueType->nullable());
            case IntegerType::class:
                return ParquetSchema\MapValue::int64(!$mapValueType->nullable());
            case StringType::class:
                return ParquetSchema\MapValue::string(!$mapValueType->nullable());
            case BooleanType::class:
                return ParquetSchema\MapValue::boolean(!$mapValueType->nullable());
            case UuidType::class:
                return ParquetSchema\MapValue::uuid(!$mapValueType->nullable());
            case DateType::class:
                return ParquetSchema\MapValue::date(!$mapValueType->nullable());
            case TimeType::class:
                return ParquetSchema\MapValue::time(!$mapValueType->nullable());
            case DateTimeType::class:
                return ParquetSchema\MapValue::datetime(!$mapValueType->nullable());
            case JsonType::class:
                return ParquetSchema\MapValue::json(!$mapValueType->nullable());
            case XMLType::class:
            case XMLElementType::class:
                return ParquetSchema\MapValue::string(!$mapValueType->nullable());
            case ObjectType::class:
                $class = $mapValueType->class;

                if (\is_a($class, \DateTimeInterface::class, true)) {
                    return ParquetSchema\MapValue::datetime(!$mapValueType->nullable());
                }

                if ($class === Uuid::class) {
                    return ParquetSchema\MapValue::string(!$mapValueType->nullable());
                }

                if ($class === \DateInterval::class) {
                    return ParquetSchema\MapValue::time(!$mapValueType->nullable());
                }

                throw new \Flow\Parquet\Exception\RuntimeException($class . ' can\'t be converted to any parquet columns.');
            case ListType::class:
                return ParquetSchema\MapValue::list($this->flowListToParquetList($mapValueType->element()), !$mapValueType->nullable());
            case MapType::class:
                return ParquetSchema\MapValue::map(
                    $this->flowMapKeyToParquetMapKey($mapValueType->key()),
                    $this->flowMapValueToParquetMapValue($mapValueType->value()),
                    !$mapValueType->nullable()
                );
            case StructureType::class:
                return ParquetSchema\MapValue::structure($this->flowStructureToParquetStructureElements($mapValueType), !$mapValueType->nullable());
        }

        throw new RuntimeException($mapValueType::class . ' is not supported.');
    }

    private function flowObjectToParquetFlat(ObjectType $type, string $name) : FlatColumn
    {
        $class = $type->class;

        if ($class === \DateInterval::class) {
            return FlatColumn::time($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
        }

        throw new RuntimeException($type->toString() . ' can\'t be converted to any parquet columns.');
    }

    private function flowStructureToParquetStructureElements(StructureType $structureType) : array
    {
        $elements = [];

        foreach ($structureType->elements() as $elementName => $elementType) {
            $elements[] = $this->flowTypeToParquetType($elementName, $elementType);
        }

        return $elements;
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowTypeToParquetType(string $name, Type $type) : Column
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
            case ObjectType::class:
                return $this->flowObjectToParquetFlat($type, $name);
            case ListType::class:
                return NestedColumn::list($name, $this->flowListToParquetList($type->element()), $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case MapType::class:
                return NestedColumn::map(
                    $name,
                    $this->flowMapKeyToParquetMapKey($type->key()),
                    $this->flowMapValueToParquetMapValue($type->value()),
                    $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED
                );
            case StructureType::class:
                return NestedColumn::struct($name, $this->flowStructureToParquetStructureElements($type), $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
        }

        throw new RuntimeException($type::class . ' is not supported.');
    }

    private function fromParquetColumnToFlowDefinition(Column $column) : Schema\Definition
    {
        if ($column instanceof FlatColumn) {
            return $this->parquetFlatToFlowType($column);
        }

        /** @var NestedColumn $column */
        return $this->parquetNestedToFlowType($column);
    }

    private function parquetFlatToFlowType(FlatColumn $column) : Schema\Definition
    {
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

    private function parquetNestedToFlowType(NestedColumn $column) : Schema\Definition
    {
        $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

        if ($column->isList()) {
            return list_schema(
                $column->name(),
                type_list(
                    $this->fromParquetColumnToFlowDefinition($column->getListElement())->type(),
                    $nullable
                )
            );
        }

        if ($column->isMap()) {
            $keyType = $this->fromParquetColumnToFlowDefinition($column->getMapKeyColumn())->type();

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            return map_schema(
                $column->name(),
                type_map(
                    $keyType,
                    $this->fromParquetColumnToFlowDefinition($column->getMapValueColumn())->type(),
                    $nullable
                )
            );
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->fromParquetColumnToFlowDefinition($structColumn)->type();
        }

        return struct_schema($column->name(), type_structure($elements, $nullable));
    }
}
