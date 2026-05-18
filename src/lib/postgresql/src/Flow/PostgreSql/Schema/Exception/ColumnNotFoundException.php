<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exception;

use function sprintf;

final class ColumnNotFoundException extends SchemaException
{
    public static function inTable(string $columnName, string $tableName): self
    {
        return new self(sprintf('Column "%s" not found in table "%s".', $columnName, $tableName));
    }
}
