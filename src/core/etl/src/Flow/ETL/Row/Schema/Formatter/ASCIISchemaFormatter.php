<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\NativeType;
use Flow\ETL\Row\Entry\StructureEntry;
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
     * @param array<string> $buffer
     *
     * @return array<string>
     */
    private function formatEntry(Schema\Definition $definition, array $buffer) : array
    {
        $entry = $definition->entry()->name();

        $type = match (\count($definition->types())) {
            1 => $definition->types()[0],
            default => '[' . \implode(', ', $definition->types()) . ']'
        };

        $indention = '';

        $buffer[] = $indention . '|-- ' . $entry . ': ' . $type . ' (nullable = ' . ($definition->isNullable() ? 'true' : 'false') . ')';

        if (\in_array(StructureEntry::class, $definition->types(), true)) {
            /** @var StructureType $structureType */
            $structureType = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

            $fields = [];

            foreach ($structureType->elements() as $structEntry) {
                $fields += $this->formatStructureElement($structEntry, $fields, 1);
            }

            $buffer = \array_merge($buffer, $fields);
        }

        return $buffer;
    }

    private function formatStructureElement(StructureElement $element, array $buffer, int $level) : array
    {
        $entry = $element->name();
        $structureType = $element->type();
        $optional = $structureType instanceof NativeType && $structureType->nullable();

        $indention = \str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        $buffer[] = $indention . '|-- ' . $entry . ': ' . $structureType->toString() . ' (nullable = ' . ($optional ? 'true' : 'false') . ')';

        if ($structureType instanceof StructureType) {
            $fields = [];

            foreach ($structureType->elements() as $structEntry) {
                $fields += $this->formatStructureElement($structEntry, $fields, $level + 1);
            }

            $buffer = \array_merge($buffer, $fields);
        }

        return $buffer;
    }
}
