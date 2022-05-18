<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\data\DataField;
use codename\parquet\data\DataType;
use codename\parquet\data\DateTimeDataField;
use codename\parquet\data\Field;
use codename\parquet\data\Schema as ParquetSchema;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Row\Schema as FlowSchema;
use Flow\ETL\Row\Schema\Definition;

final class SchemaConverter
{
    public function __construct()
    {
    }

    public function toParquet(FlowSchema $schema) : ParquetSchema
    {
        /** @var array<Field> $parquetSchema */
        $parquetSchema = [];

        foreach ($schema->definitions() as $definition) {
            if (\count($definition->types()) === 2 && $definition->isNullable()) {
                /** @var class-string<Entry> $type */
                $type = \current(\array_diff($definition->types(), [NullEntry::class]));
                $parquetSchema[] = $this->convertType($type, $definition);
            }

            if (\count($definition->types()) === 1) {
                $type = \current($definition->types());
                $parquetSchema[] = $this->convertType($type, $definition);
            }

            if ((\count($definition->types()) === 2 && !$definition->isNullable()) || \count($definition->types()) > 2) {
                throw new RuntimeException('Union types are not supported yet. Invalid type: ' . $definition->entry());
            }
        }

        return new ParquetSchema($parquetSchema);
    }

    /**
     * @param class-string<Entry> $type
     * @param Definition $definition
     *
     * @throws RuntimeException
     *
     * @return Field
     */
    private function convertType(string $type, Definition $definition) : Field
    {
        if ($type === ListEntry::class) {
            $listType = $definition->metadata()->get(FlowSchema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

            if ($listType instanceof ScalarType) {
                return match ($listType) {
                    ScalarType::string => new DataField($definition->entry(), DataType::String, true, true),
                    ScalarType::integer => new DataField($definition->entry(), DataType::Int32, true, true),
                    ScalarType::float => new DataField($definition->entry(), DataType::Float, true, true),
                    ScalarType::boolean => new DataField($definition->entry(), DataType::Boolean, true, true),
                };
            }

            if ($listType instanceof ObjectType) {
                if (\is_a($listType->class, \DateTimeInterface::class, true)) {
                    return new DateTimeDataField($definition->entry(), DataType::DateTimeOffset, true, true);
                }

                throw new RuntimeException("List of {$listType->class} is not supported yet supported.");
            }
        }

        return match ($type) {
            ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry"),
            StringEntry::class => new DataField($definition->entry(), DataType::String, true),
            JsonEntry::class => new DataField($definition->entry(), DataType::String, true),
            IntegerEntry::class => new DataField($definition->entry(), DataType::Int32, true),
            FloatEntry::class => new DataField($definition->entry(), DataType::Float, true),
            BooleanEntry::class => new DataField($definition->entry(), DataType::Boolean, true),
            DateTimeEntry::class => new DateTimeDataField($definition->entry(), DataType::DateTimeOffset, true),
            default => throw new RuntimeException($type . ' is not yet supported.')
        };
    }
}
