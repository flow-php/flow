<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\{ValueConverter};

/**
 * Multirange type converter for PostgreSQL 14+.
 */
final class MultirangeConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        return '';
    }
}
