<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

/**
 * Escapes string values for use in PostgreSQL array literals.
 *
 * PostgreSQL array syntax rules:
 * - Elements are separated by commas
 * - Elements containing commas, double quotes, backslashes, curly braces,
 *   or whitespace must be double-quoted
 * - Within double quotes: backslash → \\, double quote → \"
 * - Empty strings must be quoted
 * - String "NULL" must be quoted to distinguish from SQL NULL
 */
final class StringEscaper
{
    /**
     * Escapes a string value for inclusion in a PostgreSQL array literal.
     */
    public static function escape(string $value): string
    {
        if ($value === '') {
            return '""';
        }

        $needsQuoting = \preg_match('/[,"{}\\\\\s]/', $value) === 1 || \strcasecmp($value, 'NULL') === 0;

        if (!$needsQuoting) {
            return $value;
        }

        $escaped = \str_replace(['\\', '"'], ['\\\\', '\\"'], $value);

        return '"' . $escaped . '"';
    }

    /**
     * Escapes a value that is already known to need quoting (e.g., JSON).
     * Skips the needsQuoting check for performance.
     */
    public static function escapeAlwaysQuoted(string $value): string
    {
        $escaped = \str_replace(['\\', '"'], ['\\\\', '\\"'], $value);

        return '"' . $escaped . '"';
    }
}
