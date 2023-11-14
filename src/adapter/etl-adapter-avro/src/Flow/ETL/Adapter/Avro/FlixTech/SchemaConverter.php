<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\FlowMetadata;

final class SchemaConverter
{
    public function toAvroJsonSchema(Schema $schema) : string
    {
        $fields = [];

        foreach ($schema->definitions() as $definition) {
            if (!\AvroName::is_well_formed_name($definition->entry()->name())) {
                throw new RuntimeException(
                    'Avro support only entry with names matching following regular expression: "' . \AvroName::NAME_REGEXP . '", entry "' . $definition->entry() . '" does not match it. Consider using DataFrame::rename method before writing to Avro format.'
                );
            }

            $fields[] = $this->convert($definition);
        }

        return \json_encode(['name' => 'row', 'type' => 'record', 'fields' => $fields], flags: \JSON_THROW_ON_ERROR);
    }

    /**
     * @return array{name: string, type: string|string[]}
     */
    private function convert(Definition $definition) : array
    {
        $type = $this->typeFromDefinition($definition);

        if ($type === ListEntry::class) {
            /** @var ListType $listType */
            $listType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);
            $listElement = $listType->element();

            if ($listElement->value() instanceof ScalarType) {
                return match ($listElement->value()->toString()) {
                    ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                    ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                    ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                    ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                    default => throw new RuntimeException('List of ' . $listElement->value()->toString() . ' is not supported yet supported.'),
                };
            }

            if ($listElement->value() instanceof ObjectType) {
                if (\is_a($listElement->value()->class, \DateTimeInterface::class, true)) {
                    return ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros']];
                }
            }

            throw new RuntimeException("List of {$listElement->toString()} is not supported yet supported.");
        }

        if ($type === Entry\MapEntry::class) {
            /** @var MapType $mapType */
            $mapType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_MAP_ENTRY_TYPE);

            return match ($mapType->value()->value()->toString()) {
                ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::STRING_TYPE]],
                ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::INT_TYPE]],
                ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::FLOAT_TYPE]],
                ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::BOOLEAN_TYPE]],
                default => throw new RuntimeException('Map ' . $mapType->toString() . ' is not supported yet supported.'),
            };
        }

        if ($type === StructureEntry::class) {
            /** @var StructureType $structureType */
            $structureType = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

            $structConverter = function (array $definitions) use (&$structConverter) : array {
                $structureFields = [];

                /** @var StructureElement $structureElement */
                foreach ($definitions as $structureElement) {
                    $type = $structureElement->type();

                    if ($type instanceof StructureType) {
                        $structureFields[] = [
                            'name' => $structureElement->name(),
                            'type' => [
                                'name' => \ucfirst($structureElement->name()),
                                'type' => \AvroSchema::RECORD_SCHEMA,
                                'fields' => $structConverter($type->elements()),
                            ],
                        ];
                    } else {
                        $structureFields[] = $this->structureElementToArvo($structureElement);
                    }
                }

                return $structureFields;
            };

            return [
                'name' => $definition->entry()->name(),
                'type' => ['name' => \ucfirst($definition->entry()->name()), 'type' => \AvroSchema::RECORD_SCHEMA, 'fields' => $structConverter($structureType->elements())],
            ];
        }

        $avroType = match ($type) {
            Entry\StringEntry::class, Entry\JsonEntry::class, Entry\UuidEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::STRING_TYPE],
            Entry\EnumEntry::class => [
                'name' => $definition->entry()->name(),
                'type' => [
                    'name' => $definition->entry()->name(),
                    'type' => \AvroSchema::ENUM_SCHEMA,
                    'symbols' => \array_map(
                        fn (\UnitEnum $e) => $e->name,
                        $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CASES)
                    ),
                ],
            ],
            Entry\IntegerEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::INT_TYPE],
            Entry\FloatEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::FLOAT_TYPE],
            Entry\BooleanEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::BOOLEAN_TYPE],
            Entry\ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Avro file, try convert it to ListEntry"),
            Entry\DateTimeEntry::class => ['name' => $definition->entry()->name(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'],
            Entry\NullEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::NULL_TYPE],
            default => throw new RuntimeException($type . ' is not yet supported.')
        };

        if ($definition->isNullable()) {
            $avroType['type'] = [$avroType['type'], \AvroSchema::NULL_TYPE];
        }

        return $avroType;
    }

    private function structureElementToArvo(StructureElement $element) : array
    {
        $elementType = $element->type();

        if ($elementType instanceof ScalarType) {
            if ($elementType->isString()) {
                return ['name' => $element->name(), 'type' => \AvroSchema::STRING_TYPE];
            }

            if ($elementType->isInteger()) {
                return ['name' => $element->name(), 'type' => \AvroSchema::INT_TYPE];
            }

            if ($elementType->isFloat()) {
                return ['name' => $element->name(), 'type' => \AvroSchema::FLOAT_TYPE];
            }

            if ($elementType->isBoolean()) {
                return ['name' => $element->name(), 'type' => \AvroSchema::BOOLEAN_TYPE];
            }
        }

        if ($elementType instanceof ArrayType) {
            throw new RuntimeException("ArrayEntry entry can't be saved in Avro file, try convert it to ListEntry, MapEntry or StructEntry");
        }

        if ($elementType instanceof ObjectType) {
            if (\in_array($elementType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                return ['name' => $element->name(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'];
            }

            if ($elementType->class === Entry\Type\Uuid::class) {
                return ['name' => $element->name(), 'type' => \AvroSchema::STRING_TYPE];
            }

            throw new RuntimeException($elementType->class . ' is not supported.');
        }

        if ($elementType instanceof ListType) {
            $listElement = $elementType->element();

            return match ($listElement->value()->toString()) {
                ScalarType::STRING => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                ScalarType::INTEGER => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                ScalarType::FLOAT => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                ScalarType::BOOLEAN => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                default => throw new RuntimeException('List of ' . $listElement->value()->toString() . ' is not supported yet supported.'),
            };
        }

        throw new RuntimeException($element->toString() . ' is not yet supported.');
    }

    private function typeFromDefinition(Definition $definition) : string
    {
        if ($definition->isNullable() && \count($definition->types()) === 2) {
            /** @var class-string<Entry> $type */
            $type = \current(\array_diff($definition->types(), [NullEntry::class]));
        } elseif (\count($definition->types()) === 1) {
            $type = \current($definition->types());
        } else {
            throw new RuntimeException('Union types are not supported by Avro file format. Invalid type: ' . $definition->entry()->name());
        }

        return $type;
    }
}
