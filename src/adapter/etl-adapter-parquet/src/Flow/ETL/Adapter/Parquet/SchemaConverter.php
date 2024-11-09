<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\{bool_schema,
    datetime_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    object_schema,
    str_schema,
    struct_schema,
    struct_type,
    structure_element,
    type_list,
    type_map,
    type_object,
    uuid_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, JsonType, ListType, MapType, StructureType, UuidType, XMLElementType, XMLType};
use Flow\ETL\PHP\Type\Native\{ObjectType, ScalarType};
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

    private function flowListToParquetList(ListType $type) : ListElement
    {
        $element = $type->element()->type();

        switch ($element::class) {
            case ScalarType::class:
                switch ($element->type()) {
                    case ScalarType::FLOAT:
                        return ListElement::float(!$element->nullable());
                    case ScalarType::INTEGER:
                        return ListElement::int64(!$element->nullable());
                    case ScalarType::STRING:
                        return ListElement::string(!$element->nullable());
                    case ScalarType::BOOLEAN:
                        return ListElement::boolean(!$element->nullable());
                }

                break;
            case DateTimeType::class:
                return ListElement::datetime(!$element->nullable());
            case UuidType::class:
                return ListElement::uuid(!$element->nullable());
            case JsonType::class:
                return ListElement::json(!$element->nullable());
            case XMLType::class:
            case XMLElementType::class:
                return ListElement::string(!$element->nullable());
            case ObjectType::class:
                $class = $element->class;

                if ($class === \DateInterval::class) {
                    return ListElement::time(!$element->nullable());
                }

                throw new \Flow\Parquet\Exception\RuntimeException($class . ' can\'t be converted to any parquet columns.');
            case ListType::class:
                return ListElement::list($this->flowListToParquetList($element), !$element->nullable());
            case MapType::class:
                return ListElement::map(
                    $this->flowMapKeyToParquetMapKey($element->key()),
                    $this->flowMapValueToParquetMapValue($element->value()),
                    !$type->nullable()
                );
            case StructureType::class:
                return ListElement::structure($this->flowStructureToParquetStructureElements($element), !$element->nullable());
        }

        throw new RuntimeException($element::class . ' is not supported.');
    }

    private function flowMapKeyToParquetMapKey(MapKey $mapKey) : ParquetSchema\MapKey
    {
        $mapKeyType = $mapKey->type();

        switch ($mapKeyType::class) {
            case UuidType::class:
                return ParquetSchema\MapKey::uuid();
            case DateTimeType::class:
                return ParquetSchema\MapKey::datetime();
            case ScalarType::class:
                switch ($mapKeyType->type()) {
                    case ScalarType::FLOAT:
                        return ParquetSchema\MapKey::float();
                    case ScalarType::INTEGER:
                        return ParquetSchema\MapKey::int64();
                    case ScalarType::STRING:
                        return ParquetSchema\MapKey::string();
                    case ScalarType::BOOLEAN:
                        return ParquetSchema\MapKey::boolean();
                }

                break;
        }

        throw new RuntimeException($mapKeyType::class . ' is not supported.');
    }

    private function flowMapValueToParquetMapValue(MapValue $mapValue) : ParquetSchema\MapValue
    {
        $mapValueType = $mapValue->type();

        switch ($mapValueType::class) {
            case ScalarType::class:
                switch ($mapValueType->type()) {
                    case ScalarType::FLOAT:
                        return ParquetSchema\MapValue::float(!$mapValueType->nullable());
                    case ScalarType::INTEGER:
                        return ParquetSchema\MapValue::int64(!$mapValueType->nullable());
                    case ScalarType::STRING:
                        return ParquetSchema\MapValue::string(!$mapValueType->nullable());
                    case ScalarType::BOOLEAN:
                        return ParquetSchema\MapValue::boolean(!$mapValueType->nullable());
                }

                break;
            case UuidType::class:
                return ParquetSchema\MapValue::uuid(!$mapValueType->nullable());
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
                return ParquetSchema\MapValue::list($this->flowListToParquetList($mapValueType), !$mapValueType->nullable());
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

    private function flowScalarToParquetFlat(ScalarType $type, string $name) : FlatColumn
    {
        switch ($type->type()) {
            case ScalarType::FLOAT:
                return FlatColumn::float($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case ScalarType::INTEGER:
                return FlatColumn::int64($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case ScalarType::STRING:
                return FlatColumn::string($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
            case ScalarType::BOOLEAN:
                return FlatColumn::boolean($name, $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);

            default:
                throw new RuntimeException($type->type() . ' is not supported.');
        }
    }

    private function flowStructureToParquetStructureElements(StructureType $structureType) : array
    {
        $elements = [];

        foreach ($structureType->elements() as $element) {
            $elements[] = $this->flowTypeToParquetType($element->name(), $element->type());
        }

        return $elements;
    }

    private function flowTypeToParquetType(string $name, Type $type) : Column
    {
        switch ($type::class) {
            case ScalarType::class:
                return $this->flowScalarToParquetFlat($type, $name);
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
                return NestedColumn::list($name, $this->flowListToParquetList($type), $type->nullable() ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED);
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
                    ParquetSchema\ConvertedType::DATE => datetime_schema($column->name(), $nullable),
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
            ParquetSchema\LogicalType::DATE => datetime_schema($column->name(), $nullable),
            ParquetSchema\LogicalType::TIME => object_schema($column->name(), type_object(\DateInterval::class, $nullable)),
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

            if (!$keyType instanceof ScalarType) {
                throw new RuntimeException('Flow expects map key type to be scalar type.');
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

        /** @var array<StructureElement> $elements */
        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[] = structure_element(
                $structColumn->name(),
                $this->fromParquetColumnToFlowDefinition($structColumn)->type()
            );
        }

        return struct_schema($column->name(), struct_type($elements, $nullable));
    }
}
