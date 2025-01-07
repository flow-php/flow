<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Type;
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

            foreach ($structureType->elements() as $name => $type) {
                $fields += $this->formatStructureElement($name, $type, $fields, 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $entry . ': ' . $definition->type()->toString();
        }

        return $buffer;
    }

    /**
     * @param Type<mixed> $structureType
     */
    private function formatStructureElement(string $name, Type $structureType, array $buffer, int $level) : array
    {

        $indention = \str_repeat('    ', $level);

        if ($indention !== '') {
            $indention = '|' . $indention;
        }

        if ($structureType instanceof StructureType) {
            $buffer[] = $indention . '|-- ' . $name . ': structure';

            $fields = [];

            foreach ($structureType->elements() as $nextName => $nextType) {
                $fields += $this->formatStructureElement($nextName, $nextType, $fields, $level + 1);
            }

            $buffer = \array_merge($buffer, $fields);
        } else {
            $buffer[] = $indention . '|-- ' . $name . ': ' . $structureType->toString();
        }

        return $buffer;
    }
}
