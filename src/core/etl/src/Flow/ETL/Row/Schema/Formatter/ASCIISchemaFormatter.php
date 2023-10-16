<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
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
    private function formatEntry(Schema\Definition $definition, array $buffer, int $level = 0) : array
    {
        $entry = $definition->entry()->name();

        $type = match (\count($definition->types())) {
            1 => $definition->types()[0],
            default => '[' . \implode(', ', $definition->types()) . ']'
        };

        $indention = \str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        $buffer[] = $indention . '|-- ' . $entry . ': ' . $type . ' (nullable = ' . ($definition->isNullable() ? 'true' : 'false') . ')';

        if (\in_array(StructureEntry::class, $definition->types(), true)) {
            /** @var array<Definition> $structureEntries */
            $structureEntries = $definition->metadata()->get(FlowMetadata::METADATA_STRUCTURE_DEFINITIONS);

            $fields = [];

            foreach ($structureEntries as $structEntry) {
                $fields += $this->formatEntry($structEntry, $fields, $level + 1);
            }

            $buffer = \array_merge($buffer, $fields);
        }

        return $buffer;
    }
}
