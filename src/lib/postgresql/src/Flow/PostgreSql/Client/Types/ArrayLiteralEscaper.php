<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use function preg_match;
use function str_replace;
use function strcasecmp;

final class ArrayLiteralEscaper
{
    /**
     * Escapes a string value for inclusion in a PostgreSQL array literal.
     */
    public static function escape(string $value): string
    {
        if ($value === '') {
            return '""';
        }

        $needsQuoting = preg_match('/[,"{}\\\\\s]/', $value) === 1 || strcasecmp($value, 'NULL') === 0;

        if (!$needsQuoting) {
            return $value;
        }

        $escaped = str_replace(['\\', '"'], ['\\\\', '\\"'], $value);

        return '"' . $escaped . '"';
    }

    /**
     * Escapes a value that is already known to need quoting (e.g., JSON).
     * Skips the needsQuoting check for performance.
     */
    public static function escapeAlwaysQuoted(string $value): string
    {
        $escaped = str_replace(['\\', '"'], ['\\\\', '\\"'], $value);

        return '"' . $escaped . '"';
    }
}
