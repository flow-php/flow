<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\data\DataField;
use codename\parquet\data\DataType;
use codename\parquet\data\DateTimeDataField;
use codename\parquet\data\Field;
use codename\parquet\data\Schema as ParquetSchema;
use codename\parquet\data\StructField;
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
            $parquetSchema[] = $this->convert($definition);
        }

        return new ParquetSchema($parquetSchema);
    }

    /**
     * @param Definition $definition
     *
     * @throws RuntimeException
     *
     * @return Field
     *
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedFunctionCall
     * @psalm-suppress MixedArgumentTypeCoercion
     * @psalm-suppress RedundantConditionGivenDocblockType
     */
    private function convert(Definition $definition) : Field
    {
        $type = $this->typeFromDefinition($definition);

        if ($type === ListEntry::class) {
            $listType = $definition->metadata()->get(FlowSchema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

            if ($listType instanceof ScalarType) {
                return match ($listType) {
                    ScalarType::string => new DataField($definition->entry()->name(), DataType::String, true, true),
                    ScalarType::integer => new DataField($definition->entry()->name(), DataType::Int32, true, true),
                    ScalarType::float => new DataField($definition->entry()->name(), DataType::Float, true, true),
                    ScalarType::boolean => new DataField($definition->entry()->name(), DataType::Boolean, true, true),
                };
            }

            if ($listType instanceof ObjectType) {
                if (\is_a($listType->class, \DateTimeInterface::class, true)) {
                    return new DateTimeDataField($definition->entry()->name(), DataType::DateTimeOffset, true, true);
                }

                throw new RuntimeException("List of {$listType->class} is not supported yet supported.");
            }
        }

        if ($type === Entry\StructureEntry::class) {
            /** @var array<string, Definition> $structureDefinitions */
            $structureDefinitions = $definition->metadata()->get(FlowSchema\FlowMetadata::METADATA_STRUCTURE_DEFINITIONS);

            $structConverter = function (array $definitions) use (&$structConverter) : array {
                $structureFields = [];

                /** @var array<Definition>|Definition $definition */
                foreach ($definitions as $name => $definition) {
                    if (!\is_array($definition)) {
                        $structureFields[$name] = $this->convert($definition);
                    } else {
                        $structureFields[$name] = StructField::createWithFieldArray($name, $structConverter($definition), false);
                    }
                }

                return $structureFields;
            };

            return StructField::createWithFieldArray($definition->entry()->name(), $structConverter($structureDefinitions), $definition->isNullable(), false);
        }

        return match ($type) {
            ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry"),
            StringEntry::class => new DataField($definition->entry()->name(), DataType::String, true),
            JsonEntry::class => new DataField($definition->entry()->name(), DataType::String, true),
            IntegerEntry::class => new DataField($definition->entry()->name(), DataType::Int32, true),
            FloatEntry::class => new DataField($definition->entry()->name(), DataType::Float, true),
            BooleanEntry::class => new DataField($definition->entry()->name(), DataType::Boolean, true),
            DateTimeEntry::class => new DateTimeDataField($definition->entry()->name(), DataType::DateTimeOffset, true),
            Entry\UuidEntry::class => new DataField($definition->entry()->name(), DataType::String, true),
            default => throw new RuntimeException($type . ' is not yet supported.')
        };
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
