<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exception;

use function sprintf;

final class TableNotFoundException extends SchemaException
{
    public static function inSchema(string $tableName, string $schemaName): self
    {
        return new self(sprintf('Table "%s" not found in schema "%s".', $tableName, $schemaName));
    }
}
