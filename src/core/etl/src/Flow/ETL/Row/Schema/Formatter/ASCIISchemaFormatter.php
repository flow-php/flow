<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\Row\Schema;
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

        $indention = '';

        if ($definition->type() instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $entry . ': structure';

            /** @var StructureType $structureType */
            $structureType = $definition->type();

            $fields = [];

            foreach ($structureType->elements() as $structEntry) {
                $fields += $this->formatStructureElement($structEntry, $fields, 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $entry . ': ' . $definition->type()->toString();
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
