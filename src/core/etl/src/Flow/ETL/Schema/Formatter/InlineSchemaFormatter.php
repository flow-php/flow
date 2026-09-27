<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter;

use Flow\ETL\Schema;
use Flow\ETL\Schema\SchemaFormatter;

use function implode;

final readonly class InlineSchemaFormatter implements SchemaFormatter
{
    public function format(Schema $schema): string
    {
        $columns = [];

        foreach ($schema->definitions() as $name => $definition) {
            $columns[] = $name . ': ' . ($definition->isNullable() ? '?' : '') . $definition->type()->toString();
        }

        return implode(', ', $columns);
    }
}
