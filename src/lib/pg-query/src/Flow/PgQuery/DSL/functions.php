<?php

declare(strict_types=1);

namespace Flow\PgQuery\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\PgQuery\AST\Transformers\{CountModifier, KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, PaginationConfig, PaginationModifier, SortOrder};
use Flow\PgQuery\{DeparseOptions, ParsedQuery, Parser};

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_parser() : Parser
{
    return new Parser();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_parse(string $sql) : ParsedQuery
{
    return (new Parser())->parse($sql);
}

/**
 * Returns a fingerprint of the given SQL query.
 * Literal values are normalized so they won't affect the fingerprint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_fingerprint(string $sql) : ?string
{
    return (new Parser())->fingerprint($sql);
}

/**
 * Normalize SQL query by replacing literal values and named parameters with positional parameters.
 * WHERE id = :id will be changed into WHERE id = $1
 * WHERE id = 1 will be changed into WHERE id = $1.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_normalize(string $sql) : ?string
{
    return (new Parser())->normalize($sql);
}

/**
 * Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).
 * This handles DDL statements differently from pg_normalize() which is optimized for DML.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_normalize_utility(string $sql) : ?string
{
    return (new Parser())->normalizeUtility($sql);
}

/**
 * Split string with multiple SQL statements into array of individual statements.
 *
 * @return array<string>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_split(string $sql) : array
{
    return (new Parser())->split($sql);
}

/**
 * Create DeparseOptions for configuring SQL formatting.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_deparse_options() : DeparseOptions
{
    return DeparseOptions::new();
}

/**
 * Convert a ParsedQuery AST back to SQL string.
 *
 * When called without options, returns the SQL as a simple string.
 * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
 *
 * @throws \RuntimeException if deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_deparse(ParsedQuery $query, ?DeparseOptions $options = null) : string
{
    return $query->deparse($options);
}

/**
 * Parse and format SQL query with pretty printing.
 *
 * This is a convenience function that parses SQL and returns it formatted.
 *
 * @param string $sql The SQL query to format
 * @param null|DeparseOptions $options Formatting options (defaults to pretty-print enabled)
 *
 * @throws \RuntimeException if parsing or deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_format(string $sql, ?DeparseOptions $options = null) : string
{
    return (new Parser())->parse($sql)->deparse($options ?? DeparseOptions::new());
}

/**
 * Generate a summary of parsed queries in protobuf format.
 * Useful for query monitoring and logging without full AST overhead.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_summary(string $sql, int $options = 0, int $truncateLimit = 0) : string
{
    return (new Parser())->summary($sql, $options, $truncateLimit);
}

/**
 * Transform a SQL query into a paginated query with LIMIT and OFFSET.
 *
 * @param string $sql The SQL query to paginate
 * @param int $limit Maximum number of rows to return
 * @param int $offset Number of rows to skip (requires ORDER BY in query)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_to_paginated_query(string $sql, int $limit, int $offset = 0) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new PaginationModifier(new PaginationConfig($limit, $offset)));

    return $query->deparse();
}

/**
 * Transform a SQL query into a COUNT query for pagination.
 *
 * Wraps the query in: SELECT COUNT(*) FROM (...) AS _count_subq
 * Removes ORDER BY and LIMIT/OFFSET from the inner query.
 *
 * @param string $sql The SQL query to transform
 *
 * @return string The COUNT query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_to_count_query(string $sql) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new CountModifier());

    return $query->deparse();
}

/**
 * Transform a SQL query into a keyset (cursor-based) paginated query.
 *
 * More efficient than OFFSET for large datasets - uses indexed WHERE conditions.
 *
 * @param string $sql The SQL query to paginate (must have ORDER BY)
 * @param int $limit Maximum number of rows to return
 * @param list<KeysetColumn> $columns Columns for keyset pagination (must match ORDER BY)
 * @param null|list<null|bool|float|int|string> $cursor Values from last row of previous page (null for first page)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_to_keyset_query(string $sql, int $limit, array $columns, ?array $cursor = null) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new KeysetPaginationModifier(new KeysetPaginationConfig($limit, $columns, $cursor)));

    return $query->deparse();
}

/**
 * Create a PaginationConfig for offset-based pagination.
 *
 * @param int $limit Maximum number of rows to return
 * @param int $offset Number of rows to skip (requires ORDER BY in query)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_pagination_config(int $limit, int $offset = 0) : PaginationConfig
{
    return new PaginationConfig($limit, $offset);
}

/**
 * Create a PaginationModifier for offset-based pagination.
 *
 * Applies LIMIT and OFFSET to the query. OFFSET without ORDER BY will throw an exception.
 *
 * @param int $limit Maximum number of rows to return
 * @param int $offset Number of rows to skip (requires ORDER BY in query)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_pagination(int $limit, int $offset = 0) : PaginationModifier
{
    return new PaginationModifier(new PaginationConfig($limit, $offset));
}

/**
 * Create a CountModifier that transforms a SELECT query into a COUNT query.
 *
 * The original query is wrapped in: SELECT COUNT(*) FROM (...) AS _count_subq
 * ORDER BY and LIMIT/OFFSET are removed from the inner query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_count_modifier() : CountModifier
{
    return new CountModifier();
}

/**
 * Create a KeysetColumn for keyset pagination.
 *
 * @param string $column Column name (can include table alias like "u.id")
 * @param SortOrder $order Sort order (ASC or DESC)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_keyset_column(string $column, SortOrder $order = SortOrder::ASC) : KeysetColumn
{
    return new KeysetColumn($column, $order);
}

/**
 * Create a KeysetPaginationConfig for cursor-based pagination.
 *
 * @param int $limit Maximum number of rows to return
 * @param list<KeysetColumn> $columns Columns to use for keyset pagination (must match ORDER BY)
 * @param null|list<null|bool|float|int|string> $cursor Cursor values from the last row of previous page (null for first page)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_keyset_pagination_config(int $limit, array $columns, ?array $cursor = null) : KeysetPaginationConfig
{
    return new KeysetPaginationConfig($limit, $columns, $cursor);
}

/**
 * Create a KeysetPaginationModifier for cursor-based pagination.
 *
 * Keyset pagination is more efficient than OFFSET for large datasets because it uses
 * indexed WHERE conditions instead of scanning and skipping rows.
 *
 * @param int $limit Maximum number of rows to return
 * @param list<KeysetColumn> $columns Columns to use for keyset pagination (must match ORDER BY)
 * @param null|list<null|bool|float|int|string> $cursor Cursor values from the last row of previous page (null for first page)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_keyset_pagination(int $limit, array $columns, ?array $cursor = null) : KeysetPaginationModifier
{
    return new KeysetPaginationModifier(new KeysetPaginationConfig($limit, $columns, $cursor));
}
