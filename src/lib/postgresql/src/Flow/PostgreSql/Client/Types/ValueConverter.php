<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\Types\Type;

/**
 * Converts values between PHP and PostgreSQL formats.
 *
 * @template-covariant TPhp The PHP type this converter produces
 */
interface ValueConverter
{
    /**
     * Get the Flow PHP Type this converter produces.
     *
     * @return Type<TPhp>
     */
    public function flowType() : Type;

    /**
     * Get the PostgreSQL types this converter handles.
     *
     * @return array<PostgreSqlType>
     */
    public function supportedTypes() : array;

    /**
     * Convert a PHP value to PostgreSQL format for binding.
     *
     * @return null|string The value as a string for pg_query_params, or null for NULL values
     */
    public function toDatabase(mixed $value) : ?string;

    /**
     * Convert a PostgreSQL value to PHP format.
     *
     * @return TPhp
     */
    public function toPhp(string $value, PostgreSqlType $type) : mixed;
}
