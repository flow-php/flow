<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

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
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\FlowMetadata;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement as ParquetListElement;
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
            ArrayEntry::class => throw new RuntimeException("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry or StructEntry"),
            StringEntry::class => FlatColumn::string($definition->entry()->name()),
            EnumEntry::class => FlatColumn::string($definition->entry()->name()),
            ObjectEntry::class => throw new RuntimeException($type . ' is not supported.'),
            JsonEntry::class => FlatColumn::json($definition->entry()->name()),
            IntegerEntry::class => FlatColumn::int64($definition->entry()->name()),
            FloatEntry::class => FlatColumn::float($definition->entry()->name()),
            BooleanEntry::class => FlatColumn::boolean($definition->entry()->name()),
            DateTimeEntry::class => FlatColumn::dateTime($definition->entry()->name()),
            UuidEntry::class => FlatColumn::uuid($definition->entry()->name()),
            ListEntry::class => $this->listEntryToParquet($definition),
            StructureEntry::class => $this->structureEntryToParquet($definition),
            default => throw new RuntimeException($type . ' is not yet supported.')
        };
    }

    private function listEntryToParquet(Definition $definition) : NestedColumn
    {
        /** @var ListElement $listType */
        $listType = $definition->metadata()->get(FlowMetadata::METADATA_LIST_ENTRY_TYPE);

        if ($listType->value() instanceof ScalarType) {
            return NestedColumn::list(
                $definition->entry()->name(),
                match ($listType->value()) {
                    ScalarType::string => ParquetListElement::string(),
                    ScalarType::integer => ParquetListElement::int64(),
                    ScalarType::float => ParquetListElement::float(),
                    ScalarType::boolean => ParquetListElement::boolean()
                }
            );
        }

        if ($listType->value() instanceof ObjectType) {
            if (\is_a($listType->value()->class, \DateTimeInterface::class, true)) {
                return NestedColumn::list($definition->entry()->name(), ParquetListElement::datetime());
            }

            throw new RuntimeException("List of {$listType->toString()} is not supported yet supported.");
        }

        throw new RuntimeException('List of ' . $listType::class . ' is not supported yet supported.');
    }

    private function structureEntryToParquet(Definition $definition) : NestedColumn
    {
        $structureDefinitions = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_DEFINITIONS);

        $structConverter = function (array $definitions) use (&$structConverter) : array {
            $structureFields = [];

            /** @var array<Definition>|Definition $definition */
            foreach ($definitions as $name => $definition) {
                if (!\is_array($definition)) {
                    $structureFields[] = $this->convertEntry($definition);
                } else {
                    $structureFields[] = NestedColumn::struct($name, $structConverter($definition));
                }
            }

            return $structureFields;
        };

        /**
         * @psalm-suppress PossiblyInvalidArgument
         *
         * @phpstan-ignore-next-line
         */
        return NestedColumn::struct($definition->entry()->name(), $structConverter($structureDefinitions));
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
