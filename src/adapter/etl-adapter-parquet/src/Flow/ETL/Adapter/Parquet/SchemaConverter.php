<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Logical\JsonType;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Logical\XMLNodeType;
use Flow\ETL\PHP\Type\Logical\XMLType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final class SchemaConverter
{
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
                        return ListElement::float();
                    case ScalarType::INTEGER:
                        return ListElement::int64();
                    case ScalarType::STRING:
                        return ListElement::string();
                    case ScalarType::BOOLEAN:
                        return ListElement::boolean();
                }

                break;
            case DateTimeType::class:
                return ListElement::datetime();
            case UuidType::class:
                return ListElement::uuid();
            case JsonType::class:
                return ListElement::json();
            case XMLType::class:
            case XMLNodeType::class:
                return ListElement::string();
            case ObjectType::class:
                $class = $element->class;

                if ($class === \DateInterval::class) {
                    return ListElement::time();
                }

                throw new \Flow\Parquet\Exception\RuntimeException($class . ' can\'t be converted to any parquet columns.');
            case ListType::class:
                return ListElement::list($this->flowListToParquetList($element));
            case MapType::class:
                return ListElement::map(
                    $this->flowMapKeyToParquetMapKey($element->key()),
                    $this->flowMapValueToParquetMapValue($element->value())
                );
            case StructureType::class:
                return ListElement::structure($this->flowStructureToParquetStructureElements($element));
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
                        return ParquetSchema\MapValue::float();
                    case ScalarType::INTEGER:
                        return ParquetSchema\MapValue::int64();
                    case ScalarType::STRING:
                        return ParquetSchema\MapValue::string();
                    case ScalarType::BOOLEAN:
                        return ParquetSchema\MapValue::boolean();
                }

                break;
            case UuidType::class:
                return ParquetSchema\MapValue::uuid();
            case DateTimeType::class:
                return ParquetSchema\MapValue::datetime();
            case JsonType::class:
                return ParquetSchema\MapValue::json();
            case XMLType::class:
            case XMLNodeType::class:
                return ParquetSchema\MapValue::string();
            case ObjectType::class:
                $class = $mapValueType->class;

                if (\is_a($class, \DateTimeInterface::class, true)) {
                    return ParquetSchema\MapValue::datetime();
                }

                if ($class === Entry\Type\Uuid::class) {
                    return ParquetSchema\MapValue::string();
                }

                if ($class === \DateInterval::class) {
                    return ParquetSchema\MapValue::time();
                }

                throw new \Flow\Parquet\Exception\RuntimeException($class . ' can\'t be converted to any parquet columns.');
            case ListType::class:
                return ParquetSchema\MapValue::list($this->flowListToParquetList($mapValueType));
            case MapType::class:
                return ParquetSchema\MapValue::map(
                    $this->flowMapKeyToParquetMapKey($mapValueType->key()),
                    $this->flowMapValueToParquetMapValue($mapValueType->value())
                );
            case StructureType::class:
                return ParquetSchema\MapValue::structure(...$this->flowStructureToParquetStructureElements($mapValueType));
        }

        throw new RuntimeException($mapValueType::class . ' is not supported.');
    }

    private function flowObjectToParquetFlat(ObjectType $type, string $name) : FlatColumn
    {
        $class = $type->class;

        if ($class === \DateInterval::class) {
            return FlatColumn::time($name);
        }

        throw new RuntimeException($class . ' can\'t be converted to any parquet columns.');
    }

    private function flowScalarToParquetFlat(ScalarType $type, string $name) : FlatColumn
    {
        switch ($type->type()) {
            case ScalarType::FLOAT:
                return FlatColumn::float($name);
            case ScalarType::INTEGER:
                return FlatColumn::int64($name);
            case ScalarType::STRING:
                return FlatColumn::string($name);
            case ScalarType::BOOLEAN:
                return FlatColumn::boolean($name);

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
                return FlatColumn::datetime($name);
            case UuidType::class:
                return FlatColumn::uuid($name);
            case JsonType::class:
                return FlatColumn::json($name);
            case XMLType::class:
            case XMLNodeType::class:
                return FlatColumn::string($name);
            case ObjectType::class:
                return $this->flowObjectToParquetFlat($type, $name);
            case ListType::class:
                return NestedColumn::list($name, $this->flowListToParquetList($type));
            case MapType::class:
                return NestedColumn::map(
                    $name,
                    $this->flowMapKeyToParquetMapKey($type->key()),
                    $this->flowMapValueToParquetMapValue($type->value())
                );
            case StructureType::class:
                return NestedColumn::struct($name, $this->flowStructureToParquetStructureElements($type));
        }

        throw new RuntimeException($type::class . ' is not supported.');
    }
}
