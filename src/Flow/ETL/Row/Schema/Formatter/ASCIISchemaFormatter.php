<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Formatter;

use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\SchemaFormatter;

final class ASCIISchemaFormatter implements SchemaFormatter
{
    public function format(Schema $schema) : string
    {
        /** @var array<string, string> $entries */
        $entries = [];

        foreach ($schema->definitions() as $definition) {
            $type = match (\count($definition->types())) {
                1 => $definition->types()[0],
                default => '[' . \implode(', ', $definition->types()) . ']'
            };

            $entries[$definition->entry()] = '|-- ' . $definition->entry() . ': ' . $type . ' (nullable = ' . ($definition->isNullable() ? 'true' : 'false') . ')';
        }

        \ksort($entries);

        $output = "schema\n";
        $output .= \implode("\n", $entries);

        return $output;
    }
}
