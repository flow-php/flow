<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter;

use Flow\ETL\Schema;
use Flow\ETL\Schema\SchemaFormatter;

final readonly class JsonSchemaFormatter implements SchemaFormatter
{
    public function __construct(private bool $pretty = false)
    {
    }

    public function format(Schema $schema) : string
    {
        if ($this->pretty) {
            return \json_encode($schema->normalize(), JSON_THROW_ON_ERROR | \JSON_PRETTY_PRINT);
        }

        return \json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
    }
}
