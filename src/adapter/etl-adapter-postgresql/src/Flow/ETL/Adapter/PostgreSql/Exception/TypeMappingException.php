<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Exception;

use function sprintf;

final class TypeMappingException extends RuntimeException
{
    public static function unsupportedColumnType(string $pgTypeName): self
    {
        return new self(sprintf(
            'PostgreSQL type "%s" cannot be automatically mapped to a Flow type. Provide a column type override to map it explicitly.',
            $pgTypeName,
        ));
    }

    public static function unmappedColumn(string $column, string $flowTypeClass): self
    {
        return new self(sprintf(
            'Column "%s" of Flow type "%s" cannot be automatically mapped to a PostgreSQL type. Provide a type override to map it explicitly.',
            $column,
            $flowTypeClass,
        ));
    }

    public static function unsupportedFlowType(string $flowTypeClass): self
    {
        return new self(sprintf(
            'Flow type "%s" cannot be automatically mapped to a PostgreSQL column type. Provide a column type override to map it explicitly.',
            $flowTypeClass,
        ));
    }
}
