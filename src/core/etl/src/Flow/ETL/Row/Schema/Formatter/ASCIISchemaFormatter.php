<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\Type\Uuid;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Row\Entry\XMLNodeEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\FlowMetadata;
use Flow\ETL\Row\Schema\SchemaFormatter;

final class ASCIISchemaFormatter implements SchemaFormatter
{
    public function format(Schema $schema) : string
    {
        /** @var array<string, string> $buffer */
        $buffer = [];

        foreach ($schema->definitions() as $definition) {
            $buffer = $this->formatEntry($definition, $buffer);
        }

        \ksort($buffer);

        $output = "schema\n";
        $output .= \implode("\n", $buffer);

        return $output . "\n";
    }

    /**
     * @param array<class-string> $types
     *
     * @return string[]
     */
    private function definitionTypesFromEntry(array $types, bool $nullable, Schema\Metadata $metadata) : array
    {
        $definitionTypes = [];

        foreach ($types as $type) {
            /** @var Type $definitionType */
            $definitionType = match ($type) {
                ArrayEntry::class => new ArrayType($nullable),
                BooleanEntry::class => ScalarType::boolean($nullable),
                DateTimeEntry::class => ObjectType::of(\DateTimeImmutable::class, $nullable),
                EnumEntry::class => EnumType::of(\UnitEnum::class, $nullable),
                FloatEntry::class => ScalarType::float($nullable),
                IntegerEntry::class => ScalarType::integer($nullable),
                StringEntry::class, JsonEntry::class => ScalarType::string($nullable),
                ListEntry::class => $metadata->get(FlowMetadata::METADATA_LIST_ENTRY_TYPE),
                MapEntry::class => $metadata->get(FlowMetadata::METADATA_MAP_ENTRY_TYPE),
                ObjectEntry::class => $metadata->get(FlowMetadata::METADATA_OBJECT_ENTRY_TYPE),
                UuidEntry::class => ObjectType::of(Uuid::class, $nullable),
                XMLEntry::class => ObjectType::of(\DOMDocument::class, $nullable),
                XMLNodeEntry::class => ObjectType::of(\DOMElement::class, $nullable),
                // Fallback
                StructureEntry::class => new ArrayType(false),
                default => throw new InvalidArgumentException('Unknown entry type given: ' . $type)
            };

            $definitionTypes[] = $definitionType->toString();
        }

        return $definitionTypes;
    }

    /**
     * @param array<string> $buffer
     *
     * @return array<string>
     */
    private function formatEntry(Schema\Definition $definition, array $buffer) : array
    {
        $entry = $definition->entry()->name();

        $nullable = $structure = false;
        $types = $definition->types();
        $nullIndex = \array_search(NullEntry::class, $types, true);

        if (false !== $nullIndex) {
            $nullable = true;
            unset($types[$nullIndex]);
        }

        $structureIndex = \array_search(StructureEntry::class, $types, true);

        if (false !== $structureIndex) {
            $structure = true;
            unset($types[$structureIndex]);
        }

        $indention = '';

        if ($structure) {
            $buffer[] = $indention . '|-- ' . $entry . ': structure';

            /** @var StructureType $structureType */
            $structureType = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

            $fields = [];

            foreach ($structureType->elements() as $structEntry) {
                $fields += $this->formatStructureElement($structEntry, $fields, 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $entry . ': ' . \implode('|', $this->definitionTypesFromEntry($types, $nullable, $definition->metadata()));
        }

        return $buffer;
    }

    private function formatStructureElement(StructureElement $element, array $buffer, int $level) : array
    {
        $structureType = $element->type();

        $indention = \str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        if ($structureType instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $element->name() . ': structure';

            $fields = [];

            foreach ($structureType->elements() as $structEntry) {
                $fields += $this->formatStructureElement($structEntry, $fields, $level + 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $element->name() . ': ' . $structureType->toString();
        }

        return $buffer;
    }
}
