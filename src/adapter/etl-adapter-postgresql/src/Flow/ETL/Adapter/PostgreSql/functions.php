<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\LoaderOptions\{DeleteOptions, InsertOptions, UpdateOptions};
use Flow\ETL\Adapter\PostgreSql\Pagination\{Key, KeySet, Order};
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type as DSLType};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

/**
 * Create a PostgreSQL cursor extractor using server-side cursors for memory-efficient extraction.
 *
 * Uses DECLARE CURSOR + FETCH to stream data without loading entire result set into memory.
 * This is the only way to achieve true low memory extraction with PHP's ext-pgsql.
 *
 * Note: Requires a transaction context (auto-started if not in one).
 *
 * @param Client $client PostgreSQL client
 * @param SqlQuery|string $query SQL query to execute (wrapped in DECLARE CURSOR)
 * @param array<int, mixed> $parameters Positional parameters for the query
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_cursor(
    Client $client,
    string|SqlQuery $query,
    array $parameters = [],
) : PostgreSqlCursorExtractor {
    return new PostgreSqlCursorExtractor($client, $query, $parameters);
}

/**
 * Create a PostgreSQL extractor using LIMIT/OFFSET pagination.
 *
 * Suitable for smaller datasets. For large datasets, consider using keyset pagination
 * (from_pgsql_key_set) which is more efficient.
 *
 * @param Client $client PostgreSQL client
 * @param SqlQuery|string $query SQL query to execute (must have ORDER BY clause)
 * @param array<int, mixed> $parameters Positional parameters for the query
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_limit_offset(
    Client $client,
    string|SqlQuery $query,
    array $parameters = [],
) : PostgreSqlLimitOffsetExtractor {
    return new PostgreSqlLimitOffsetExtractor($client, $query, $parameters);
}

/**
 * Create a PostgreSQL extractor using keyset (cursor-based) pagination.
 *
 * More efficient than LIMIT/OFFSET for large datasets - uses indexed WHERE conditions
 * instead of skipping rows.
 *
 * @param Client $client PostgreSQL client
 * @param SqlQuery|string $query SQL query to execute (must have ORDER BY matching keyset columns)
 * @param KeySet $keySet Columns to use for keyset pagination
 * @param array<int, mixed> $parameters Positional parameters for the query
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_key_set(
    Client $client,
    string|SqlQuery $query,
    KeySet $keySet,
    array $parameters = [],
) : PostgreSqlKeySetExtractor {
    return new PostgreSqlKeySetExtractor($client, $query, $keySet, $parameters);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_asc(string $column) : Key
{
    return new Key($column, Order::ASC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_desc(string $column) : Key
{
    return new Key($column, Order::DESC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_set(Key ...$keys) : KeySet
{
    return new KeySet(...$keys);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::LOADER)]
function to_pgsql_table(
    Client $client,
    string $table,
) : PostgreSqlLoader {
    return new PostgreSqlLoader($client, $table);
}

/**
 * Create insert options for PostgreSQL loader.
 *
 * @param bool $skipConflicts If true, use ON CONFLICT DO NOTHING
 * @param list<string> $conflictColumns Column names for ON CONFLICT (columns)
 * @param null|string $conflictConstraint Constraint name for ON CONFLICT ON CONSTRAINT
 * @param list<string> $updateColumns Columns to update on conflict (empty = all non-key columns)
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_insert_options(
    bool $skipConflicts = false,
    array $conflictColumns = [],
    ?string $conflictConstraint = null,
    array $updateColumns = [],
) : InsertOptions {
    return new InsertOptions($skipConflicts, $conflictColumns, $conflictConstraint, $updateColumns);
}

/**
 * Create update options for PostgreSQL loader.
 *
 * @param list<string> $primaryKeys Columns to use in WHERE clause for matching rows
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_update_options(array $primaryKeys) : UpdateOptions
{
    return new UpdateOptions($primaryKeys);
}

/**
 * Create delete options for PostgreSQL loader.
 *
 * @param list<string> $primaryKeys Columns to use in WHERE clause for matching rows
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_delete_options(array $primaryKeys) : DeleteOptions
{
    return new DeleteOptions($primaryKeys);
}
