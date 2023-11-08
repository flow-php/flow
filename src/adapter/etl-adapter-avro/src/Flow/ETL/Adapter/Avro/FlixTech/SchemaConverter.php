<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\UuidEntry;
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

        return \json_encode([
            'name' => 'row',
            'type' => 'record',
            'fields' => $fields,
        ]);
    }

    /**
     * @return array{name: string, type: string|string[]}
     */
    private function convert(Definition $definition) : array
    {
        $type = $this->typeFromDefinition($definition);

        if ($type === ListEntry::class) {
            /** @var ListElement $listType */
            $listType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

            if ($listType->value() instanceof ScalarType) {
                return match ($listType->value()->toString()) {
                    ScalarType::STRING => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                    ScalarType::INTEGER => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                    ScalarType::FLOAT => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                    ScalarType::BOOLEAN => ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                    default => throw new RuntimeException('List of ' . $listType->value()->toString() . ' is not supported yet supported.'),
                };
            }

            if ($listType->value() instanceof ObjectType) {
                if (\is_a($listType->value()->class, \DateTimeInterface::class, true)) {
                    return ['name' => $definition->entry()->name(), 'type' => ['type' => 'array', 'items' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros']];
                }

                throw new RuntimeException("List of {$listType->toString()} is not supported yet supported.");
            }
        }

        if ($type === StructureEntry::class) {
            /** @var array<string, Definition> $structureDefinitions */
            $structureDefinitions = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_DEFINITIONS);

            $structConverter = function (array $definitions) use (&$structConverter) : array {
                $structureFields = [];

                /** @var Definition $definition */
                foreach ($definitions as $name => $definition) {
                    if (!\is_array($definition)) {
                        $structureFields[] = $this->convert($definition);
                    } else {
                        $structureFields[] = ['name' => $name, 'type' => ['name' => \ucfirst($name), 'type' => \AvroSchema::RECORD_SCHEMA, 'fields' => $structConverter($definition)]];
                    }
                }

                return $structureFields;
            };

            return ['name' => $definition->entry()->name(), 'type' => ['name' => \ucfirst($definition->entry()->name()), 'type' => \AvroSchema::RECORD_SCHEMA, 'fields' => $structConverter($structureDefinitions)]];
        }

        $avroType = match ($type) {
            StringEntry::class, JsonEntry::class, UuidEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::STRING_TYPE],
            EnumEntry::class => [
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
            IntegerEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::INT_TYPE],
            FloatEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::FLOAT_TYPE],
            BooleanEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::BOOLEAN_TYPE],
            ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Avro file, try convert it to ListEntry"),
            DateTimeEntry::class => ['name' => $definition->entry()->name(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'],
            NullEntry::class => ['name' => $definition->entry()->name(), 'type' => \AvroSchema::NULL_TYPE],
            default => throw new RuntimeException($type . ' is not yet supported.')
        };

        if ($definition->isNullable()) {
            $avroType['type'] = [$avroType['type'], \AvroSchema::NULL_TYPE];
        }

        return $avroType;
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
