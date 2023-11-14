<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\FlowMetadata;
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
            $columns[] = $this->convertEntry($definition);
        }

        return ParquetSchema::with(...$columns);
    }

    private function convertEntry(Definition $definition) : Column
    {
        $type = $this->typeFromDefinition($definition);

        return match ($type) {
            Entry\ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry or StructEntry"),
            Entry\StringEntry::class, Entry\EnumEntry::class => FlatColumn::string($definition->entry()->name()),
            Entry\JsonEntry::class => FlatColumn::json($definition->entry()->name()),
            Entry\IntegerEntry::class => FlatColumn::int64($definition->entry()->name()),
            Entry\FloatEntry::class => FlatColumn::float($definition->entry()->name()),
            Entry\BooleanEntry::class => FlatColumn::boolean($definition->entry()->name()),
            Entry\DateTimeEntry::class => FlatColumn::dateTime($definition->entry()->name()),
            Entry\UuidEntry::class => FlatColumn::uuid($definition->entry()->name()),
            Entry\ListEntry::class => $this->listEntryToParquet($definition),
            Entry\MapEntry::class => $this->mapEntryToParquet($definition),
            Entry\StructureEntry::class => $this->structureEntryToParquet($definition),
            default => throw new RuntimeException($type . ' is not yet supported.')
        };
    }

    private function listEntryToParquet(Definition $definition) : NestedColumn
    {
        /** @var ListType $listType */
        $listType = $definition->metadata()->get(FlowMetadata::METADATA_LIST_ENTRY_TYPE);
        $listElement = $listType->element();

        if ($listElement->value() instanceof ScalarType) {
            return NestedColumn::list(
                $definition->entry()->name(),
                match ($listElement->toString()) {
                    ScalarType::STRING => ListElement::string(),
                    ScalarType::INTEGER => ListElement::int64(),
                    ScalarType::FLOAT => ListElement::float(),
                    ScalarType::BOOLEAN => ListElement::boolean(),
                    default => throw new RuntimeException('List of ' . $listElement->toString() . ' is not supported yet supported.'),
                }
            );
        }

        if ($listElement->value() instanceof ObjectType) {
            if (\is_a($listElement->value()->class, \DateTimeInterface::class, true)) {
                return NestedColumn::list($definition->entry()->name(), ListElement::datetime());
            }

            throw new RuntimeException("List of {$listElement->value()->class} is not supported yet supported.");
        }

        throw new RuntimeException($listType->toString() . ' is not supported yet supported.');
    }

    private function mapEntryToParquet(Definition $definition) : NestedColumn
    {
        /** @var MapType $mapType */
        $mapType = $definition->metadata()->get(FlowMetadata::METADATA_MAP_ENTRY_TYPE);

        if ($mapType->value()->value() instanceof ScalarType) {
            return NestedColumn::map(
                $definition->entry()->name(),
                match ($mapType->key()->value()->toString()) {
                    ScalarType::STRING => ParquetSchema\MapKey::string(),
                    ScalarType::INTEGER => ParquetSchema\MapKey::int64(),
                    default => throw new RuntimeException('Map ' . $mapType->key()->toString() . ' is not supported yet supported.'),
                },
                match ($mapType->value()->value()->toString()) {
                    ScalarType::STRING => ParquetSchema\MapValue::string(),
                    ScalarType::INTEGER => ParquetSchema\MapValue::int64(),
                    ScalarType::FLOAT => ParquetSchema\MapValue::float(),
                    ScalarType::BOOLEAN => ParquetSchema\MapValue::boolean(),
                    default => throw new RuntimeException('Map ' . $mapType->value()->toString() . ' is not supported yet supported.'),
                }
            );
        }

        throw new RuntimeException($mapType->toString() . ' is not supported yet supported.');
    }

    private function structureElementToParquet(StructureElement $element) : Column
    {
        $elementType = $element->type();

        if ($elementType instanceof ScalarType) {
            if ($elementType->isString()) {
                return FlatColumn::string($element->name());
            }

            if ($elementType->isInteger()) {
                return FlatColumn::int64($element->name());
            }

            if ($elementType->isFloat()) {
                return FlatColumn::float($element->name());
            }

            if ($elementType->isBoolean()) {
                return FlatColumn::boolean($element->name());
            }
        }

        if ($elementType instanceof ArrayType) {
            throw new RuntimeException("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry or StructEntry");
        }

        if ($elementType instanceof ObjectType) {
            if (\in_array($elementType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                return FlatColumn::dateTime($element->name());
            }

            if ($elementType->class === Entry\Type\Uuid::class) {
                return FlatColumn::uuid($element->name());
            }

            throw new RuntimeException($elementType->class . ' is not supported.');
        }

        if ($elementType instanceof ListType) {
            $listElement = $elementType->element();

            if ($listElement->value() instanceof ScalarType) {
                return NestedColumn::list(
                    $element->name(),
                    match ($listElement->toString()) {
                        ScalarType::STRING => ListElement::string(),
                        ScalarType::INTEGER => ListElement::int64(),
                        ScalarType::FLOAT => ListElement::float(),
                        ScalarType::BOOLEAN => ListElement::boolean(),
                        default => throw new RuntimeException('List of ' . $listElement->toString() . ' is not supported yet supported.'),
                    }
                );
            }

            if ($listElement->value() instanceof ObjectType) {
                if (\is_a($listElement->value()->class, \DateTimeInterface::class, true)) {
                    return NestedColumn::list($element->name(), ListElement::datetime());
                }

                throw new RuntimeException("List of {$listElement->value()->class} is not supported yet supported.");
            }
        }

        if ($elementType instanceof MapType) {
            if ($elementType->value()->value() instanceof ScalarType) {
                return NestedColumn::map(
                    $element->name(),
                    match ($elementType->key()->value()->toString()) {
                        ScalarType::STRING => ParquetSchema\MapKey::string(),
                        ScalarType::INTEGER => ParquetSchema\MapKey::int64(),
                        default => throw new RuntimeException('Map ' . $elementType->key()->toString() . ' is not supported yet supported.'),
                    },
                    match ($elementType->value()->value()->toString()) {
                        ScalarType::STRING => ParquetSchema\MapValue::string(),
                        ScalarType::INTEGER => ParquetSchema\MapValue::int64(),
                        ScalarType::FLOAT => ParquetSchema\MapValue::float(),
                        ScalarType::BOOLEAN => ParquetSchema\MapValue::boolean(),
                        default => throw new RuntimeException('Map ' . $elementType->value()->toString() . ' is not supported yet supported.'),
                    }
                );
            }

            throw new RuntimeException($elementType->toString() . ' is not supported yet supported.');
        }

        throw new RuntimeException($element->toString() . ' is not yet supported.');
    }

    private function structureEntryToParquet(Definition $definition) : NestedColumn
    {
        /** @var StructureType $structureType */
        $structureType = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

        $structConverter = function (array $definitions) use (&$structConverter) : array {
            $structureFields = [];

            /** @var StructureElement $structureElement */
            foreach ($definitions as $structureElement) {
                $type = $structureElement->type();

                if ($type instanceof StructureType) {
                    $structureFields[] = NestedColumn::struct($structureElement->name(), $structConverter($type->elements()));
                } else {
                    $structureFields[] = $this->structureElementToParquet($structureElement);
                }
            }

            return $structureFields;
        };

        return NestedColumn::struct($definition->entry()->name(), $structConverter($structureType->elements()));
    }

    private function typeFromDefinition(Definition $definition) : string
    {
        if ($definition->isNullable() && \count($definition->types()) === 2) {
            /** @var class-string<Entry> $type */
            $type = \current(\array_diff($definition->types(), [NullEntry::class]));
        } elseif (\count($definition->types()) === 1) {
            $type = \current($definition->types());
        } else {
            throw new RuntimeException('Union types are not supported by Parquet file format. Invalid type: ' . $definition->entry()->name());
        }

        return $type;
    }
}
