<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Exception;

use function sprintf;

final class TypeMappingException extends RuntimeException
{
    public static function ambiguousEntryType(string $entryClass): self
    {
        return new self(sprintf(
            'Entry type "%s" is ambiguous and cannot be automatically mapped to a PostgreSQL type. Use withColumnType() to specify the target type explicitly.',
            $entryClass,
        ));
    }

    public static function unsupportedColumnType(string $pgTypeName): self
    {
        return new self(sprintf(
            'PostgreSQL type "%s" cannot be automatically mapped to a Flow type. Provide a column type override to map it explicitly.',
            $pgTypeName,
        ));
    }

    public static function unsupportedEntryType(string $entryClass): self
    {
        return new self(sprintf(
            'Entry type "%s" is not supported for PostgreSQL mapping. Use withColumnType() to specify the target type explicitly or provide a custom EntryTypeMapper.',
            $entryClass,
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
