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
function pg_query_parse(string $sql): string { return ''; }

/**
 * Parse PostgreSQL SQL and return protobuf-serialized AST.
 *
 * This is more efficient than pg_query_parse() when working with protobuf objects,
 * as it skips the JSON serialization step.
 *
 * @throws RuntimeException on parse error
 *
 * @return string Protobuf-serialized parse tree
 */
function pg_query_parse_protobuf(string $sql): string { return ''; }

/**
 * Generate fingerprint for SQL query.
 *
 * @return false|string Returns fingerprint string or FALSE on error
 */
function pg_query_fingerprint(string $sql): string|false { return ''; }

/**
 * Normalize SQL query (replace literal values with $N placeholders).
 *
 * @return false|string Returns normalized query or FALSE on error
 */
function pg_query_normalize(string $sql): string|false { return ''; }

/**
 * Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).
 *
 * This function handles DDL/utility statements differently from pg_query_normalize()
 * which is optimized for DML statements.
 *
 * @return false|string Returns normalized query or FALSE on error
 */
function pg_query_normalize_utility(string $sql): string|false { return ''; }

/**
 * Parse PL/pgSQL function.
 *
 * @throws RuntimeException on parse error
 */
function pg_query_parse_plpgsql(string $sql): string { return ''; }

/**
 * Split multiple SQL statements into an array.
 *
 * @throws RuntimeException on split error
 *
 * @return array<string> Array of individual SQL statements
 */
function pg_query_split(string $sql): array { return []; }

/**
 * Scan SQL into tokens (returns protobuf-encoded data).
 *
 * @throws RuntimeException on scan error
 *
 * @return string Protobuf-encoded scan result
 */
function pg_query_scan(string $sql): string { return ''; }

/**
 * Deparse a protobuf-serialized parse tree back to SQL.
 *
 * @param string $protobuf The protobuf-serialized parse tree (from ParseResult::serializeToString())
 *
 * @throws RuntimeException on deparse error
 *
 * @return string The SQL query string
 */
function pg_query_deparse(string $protobuf): string { return ''; }

/**
 * Deparse a protobuf-serialized parse tree back to SQL with formatting options.
 *
 * @param string $protobuf The protobuf-serialized parse tree (from ParseResult::serializeToString())
 * @param bool $pretty_print Enable pretty printing with indentation and line breaks
 * @param int $indent_size Number of spaces per indentation level (default: 4)
 * @param int $max_line_length Maximum line length before wrapping (default: 80)
 * @param bool $trailing_newline Add a trailing newline at the end (default: false)
 * @param bool $commas_start_of_line Place commas at the start of lines (default: false)
 *
 * @throws RuntimeException on deparse error
 *
 * @return string The formatted SQL query string
 */
function pg_query_deparse_opts(
    string $protobuf,
    bool $pretty_print = false,
    int $indent_size = 4,
    int $max_line_length = 80,
    bool $trailing_newline = false,
    bool $commas_start_of_line = false,
): string { return ''; }

/**
 * Generate a summary of parsed queries in protobuf format.
 *
 * Useful for query monitoring and logging without full AST overhead.
 *
 * @param string $sql The SQL query to summarize
 * @param int $options Parser options (PG_QUERY_PARSE_* constants)
 * @param int $truncate_limit Maximum length for truncated values (0 = no truncation)
 *
 * @throws RuntimeException on parse error
 *
 * @return string Protobuf-encoded summary
 */
function pg_query_summary(string $sql, int $options = 0, int $truncate_limit = 0): string { return ''; }

/**
 * Check if query contains utility statements (DDL like CREATE, ALTER, DROP)
 * without full parsing. More efficient than full parse when only checking statement type.
 *
 * @param string $sql The SQL query to check
 *
 * @return bool True if the query contains utility statements, false otherwise
 */
function pg_query_is_utility_stmt(string $sql): bool { return false; }
