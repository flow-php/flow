<?php

declare(strict_types=1);

/**
 * This file provides stubs for the pg_query extension functions for static analysis.
 * These functions are provided by the pg_query extension (flow-php/pg-query-ext).
 *
 * @codeCoverageIgnore
 */
if (!\function_exists('pg_query_parse')) {
    /**
     * Parse PostgreSQL SQL and return JSON AST.
     *
     * @throws \RuntimeException on parse error
     */
    function pg_query_parse(string $sql) : string
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Generate fingerprint for SQL query.
     *
     * @return false|string Returns fingerprint string or FALSE on error
     */
    function pg_query_fingerprint(string $sql) : string|false
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Normalize SQL query (replace literal values with $N placeholders).
     *
     * @return false|string Returns normalized query or FALSE on error
     */
    function pg_query_normalize(string $sql) : string|false
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Parse PL/pgSQL function.
     *
     * @throws \RuntimeException on parse error
     */
    function pg_query_parse_plpgsql(string $sql) : string
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Split multiple SQL statements into an array.
     *
     * @throws \RuntimeException on split error
     *
     * @return array<string> Array of individual SQL statements
     */
    function pg_query_split(string $sql) : array
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Scan SQL into tokens (returns protobuf-encoded data).
     *
     * @throws \RuntimeException on scan error
     *
     * @return string Protobuf-encoded scan result
     */
    function pg_query_scan(string $sql) : string
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Parse PostgreSQL SQL and return protobuf-serialized AST.
     *
     * This is more efficient than pg_query_parse() when working with protobuf objects,
     * as it skips the JSON serialization step.
     *
     * @throws \RuntimeException on parse error
     *
     * @return string Protobuf-serialized parse tree
     */
    function pg_query_parse_protobuf(string $sql) : string
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }

    /**
     * Deparse a protobuf-serialized parse tree back to SQL.
     *
     * @param string $protobuf The protobuf-serialized parse tree (from ParseResult::serializeToString())
     *
     * @throws \RuntimeException on deparse error
     *
     * @return string The SQL query string
     */
    function pg_query_deparse(string $protobuf) : string
    {
        throw new \RuntimeException('pg_query extension is not loaded');
    }
}
