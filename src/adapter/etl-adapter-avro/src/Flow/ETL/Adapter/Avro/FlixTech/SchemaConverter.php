<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, JsonType, ListType, MapType, StructureType, UuidType, XMLNodeType, XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType, EnumType, ObjectType, ScalarType};
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;

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
        $type = $definition->type();

        if ($type instanceof ListType) {
            $listElement = $type->element();

            if ($listElement->type() instanceof ScalarType) {
                return match ($listElement->type()->toString()) {
                    ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                    ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                    ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                    ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                    default => throw new RuntimeException('List of ' . $listElement->type()->toString() . ' is not supported yet supported.'),
                };
            }

            if ($listElement->type() instanceof DateTimeType) {
                return ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros']];
            }

            throw new RuntimeException("List of {$listElement->toString()} is not supported yet supported.");
        }

        if ($type instanceof MapType) {
            if (!$type->key()->isEqual(type_string())) {
                throw new RuntimeException('Map key can be only string, ' . $type->key()->toString() . ' is not supported.');
            }

            return match ($type->value()->type()->toString()) {
                ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::STRING_TYPE]],
                ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::INT_TYPE]],
                ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::FLOAT_TYPE]],
                ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => ['type' => 'map', 'values' => \AvroSchema::BOOLEAN_TYPE]],
                default => throw new RuntimeException('Map ' . $type->toString() . ' is not supported yet supported.'),
            };
        }

        if ($type instanceof StructureType) {
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
                'type' => ['name' => \ucfirst($definition->entry()->name()), 'type' => \AvroSchema::RECORD_SCHEMA, 'fields' => $structConverter($type->elements())],
            ];
        }

        $avroType = match ($type::class) {
            JsonType::class, UuidType::class, XMLType::class, XMLNodeType::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::STRING_TYPE],
            EnumType::class => [
                'name' => $definition->entry()->name(),
                'type' => [
                    'name' => $definition->entry()->name(),
                    'type' => \AvroSchema::ENUM_SCHEMA,
                    'symbols' => \array_map(
                        fn (\UnitEnum $e) => $e->name,
                        $definition->type()->class::cases()
                    ),
                ],
            ],
            DateTimeType::class => ['name' => $definition->entry()->name(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'],
            ScalarType::class => match ($type->type()) {
                ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::STRING_TYPE],
                ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::INT_TYPE],
                ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::FLOAT_TYPE],
                ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::BOOLEAN_TYPE],
            },
            default => throw new RuntimeException($type::class . ' is not yet supported.')
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

        if ($elementType instanceof DateTimeType) {
            return ['name' => $element->name(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'];
        }

        if ($elementType instanceof UuidType) {
            return ['name' => $element->name(), 'type' => \AvroSchema::STRING_TYPE];
        }

        if ($elementType instanceof ObjectType) {
            throw new RuntimeException($elementType->class . ' is not supported.');
        }

        if ($elementType instanceof ListType) {
            $listElement = $elementType->element();

            return match ($listElement->type()->toString()) {
                ScalarType::STRING => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                ScalarType::INTEGER => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                ScalarType::FLOAT => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                ScalarType::BOOLEAN => ['name' => $element->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                default => throw new RuntimeException('List of ' . $listElement->type()->toString() . ' is not supported yet supported.'),
            };
        }

        throw new RuntimeException($element->toString() . ' is not yet supported.');
    }
}
