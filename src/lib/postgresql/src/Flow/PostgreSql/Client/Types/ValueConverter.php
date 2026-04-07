<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

/**
 * Converts PHP values to PostgreSQL format for parameter binding.
 */
interface ValueConverter
{
    /**
     * Get the PostgreSQL types this converter handles.
     *
     * @return array<ValueType>
     */
    public function supportedTypes() : array;

    /**
     * Convert a PHP value to PostgreSQL format for binding.
     *
     * @return null|string The value as a string for pg_query_params, or null for NULL values
     */
    public function toDatabase(mixed $value) : ?string;
}
