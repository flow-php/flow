<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
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
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;

final class SchemaConverter
{
    public function toAvroJsonSchema(Schema $schema) : string
    {
        $fields = [];

        foreach ($schema->definitions() as $definition) {
            if (\count($definition->types()) === 2 && $definition->isNullable()) {
                /** @var class-string<Entry> $type */
                $type = \current(\array_diff($definition->types(), [NullEntry::class]));
                $fields[] = $this->convertType($type, $definition);
            }

            if (\count($definition->types()) === 1) {
                $type = \current($definition->types());
                $fields[] = $this->convertType($type, $definition);
            }

            if ((\count($definition->types()) === 2 && !$definition->isNullable()) || \count($definition->types()) > 2) {
                throw new RuntimeException('Union types are not supported yet. Invalid type: ' . $definition->entry());
            }
        }

        return \json_encode([
            'name' => 'row',
            'type' => 'record',
            'fields' => $fields,
        ]);
    }

    /**
     * @param class-string<Entry> $type
     * @param Definition $definition
     *
     * @return array{name: string, type: string}
     */
    private function convertType(string $type, Definition $definition) : array
    {
        if ($type === ListEntry::class) {
            $listType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

            if ($listType instanceof ScalarType) {
                return match ($listType) {
                    ScalarType::string => ['name' => $definition->entry(), 'type' => ['type' => 'array', 'items' => \AvroSchema::STRING_TYPE]],
                    ScalarType::integer => ['name' => $definition->entry(), 'type' => ['type' => 'array', 'items' => \AvroSchema::INT_TYPE]],
                    ScalarType::float => ['name' => $definition->entry(), 'type' => ['type' => 'array', 'items' => \AvroSchema::FLOAT_TYPE]],
                    ScalarType::boolean => ['name' => $definition->entry(), 'type' => ['type' => 'array', 'items' => \AvroSchema::BOOLEAN_TYPE]],
                };
            }

            if ($listType instanceof ObjectType) {
                if (\is_a($listType->class, \DateTimeInterface::class, true)) {
                    return ['name' => $definition->entry(), 'type' => ['type' => 'array', 'items' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros']];
                }

                throw new RuntimeException("List of {$listType->class} is not supported yet supported.");
            }
        }

        return match ($type) {
            StringEntry::class, JsonEntry::class => ['name' => $definition->entry(), 'type' => \AvroSchema::STRING_TYPE],
            EnumEntry::class => [
                'name' => $definition->entry(),
                'type' => [
                    'name' => $definition->entry(),
                    'type' => \AvroSchema::ENUM_SCHEMA,
                    'symbols' => \array_map(
                        fn (\UnitEnum $e) => $e->name,
                        $definition->metadata()->get(Definition::METADATA_ENUM_CASES)
                    ),
                ],
            ],
            IntegerEntry::class => ['name' => $definition->entry(), 'type' => \AvroSchema::INT_TYPE],
            FloatEntry::class => ['name' => $definition->entry(), 'type' => \AvroSchema::FLOAT_TYPE],
            BooleanEntry::class => ['name' => $definition->entry(), 'type' => \AvroSchema::BOOLEAN_TYPE],
            ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Avro file, try convert it to ListEntry"),
            DateTimeEntry::class => ['name' => $definition->entry(), 'type' => 'long', \AvroSchema::LOGICAL_TYPE_ATTR => 'timestamp-micros'],
            default => throw new RuntimeException($type . ' is not yet supported.')
        };
    }
}
