<?php

declare(strict_types=1);

/**
 * @generate-function-entries
 *
 * @generate-legacy-arginfo 80000
 */

/**
 * Parse PostgreSQL SQL and return JSON AST.
 *
 * @throws RuntimeException on parse error
 */
function pg_query_parse(string $sql) : string
{
}

/**
 * Generate fingerprint for SQL query.
 *
 * @return false|string Returns fingerprint string or FALSE on error
 */
function pg_query_fingerprint(string $sql) : string|false
{
}

/**
 * Normalize SQL query (replace literal values with $N placeholders).
 *
 * @return false|string Returns normalized query or FALSE on error
 */
function pg_query_normalize(string $sql) : string|false
{
}

/**
 * Parse PL/pgSQL function.
 *
 * @throws RuntimeException on parse error
 */
function pg_query_parse_plpgsql(string $sql) : string
{
}

/**
 * Split multiple SQL statements into an array.
 *
 * @throws RuntimeException on split error
 *
 * @return array<string> Array of individual SQL statements
 */
function pg_query_split(string $sql) : array
{
}

/**
 * Scan SQL into tokens (returns protobuf-encoded data).
 *
 * @throws RuntimeException on scan error
 *
 * @return string Protobuf-encoded scan result
 */
function pg_query_scan(string $sql) : string
{
}
