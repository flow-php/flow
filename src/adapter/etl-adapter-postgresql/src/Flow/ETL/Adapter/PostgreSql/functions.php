<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Table;

/**
 * Create a PostgreSQL cursor extractor using server-side cursors for memory-efficient extraction.
 *
 * Uses DECLARE CURSOR + FETCH to stream data without loading entire result set into memory.
 * This is the only way to achieve true low memory extraction with PHP's ext-pgsql.
 *
 * Note: Requires a transaction context (auto-started if not in one).
 *
 * @param Client $client PostgreSQL client
 * @param Sql|string $query SQL query to execute (wrapped in DECLARE CURSOR)
 * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_cursor(Client $client, string|Sql $query, array $parameters = []): PostgreSqlCursorExtractor
{
    return new PostgreSqlCursorExtractor($client, $query, $parameters);
}

/**
 * Create a PostgreSQL extractor using LIMIT/OFFSET pagination.
 *
 * Suitable for smaller datasets. For large datasets, consider using keyset pagination
 * (from_pgsql_key_set) which is more efficient.
 *
 * @param Client $client PostgreSQL client
 * @param Sql|string $query SQL query to execute (must have ORDER BY clause)
 * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_limit_offset(
    Client $client,
    string|Sql $query,
    array $parameters = [],
): PostgreSqlLimitOffsetExtractor {
    return new PostgreSqlLimitOffsetExtractor($client, $query, $parameters);
}

/**
 * Create a PostgreSQL extractor using keyset (cursor-based) pagination.
 *
 * More efficient than LIMIT/OFFSET for large datasets - uses indexed WHERE conditions
 * instead of skipping rows.
 *
 * @param Client $client PostgreSQL client
 * @param Sql|string $query SQL query to execute (must have ORDER BY matching keyset columns)
 * @param KeySet $keySet Columns to use for keyset pagination
 * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_key_set(
    Client $client,
    string|Sql $query,
    KeySet $keySet,
    array $parameters = [],
): PostgreSqlKeySetExtractor {
    return new PostgreSqlKeySetExtractor($client, $query, $keySet, $parameters);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_asc(string $column): Key
{
    return new Key($column, Order::ASC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_desc(string $column): Key
{
    return new Key($column, Order::DESC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_set(Key ...$keys): KeySet
{
    return new KeySet(...$keys);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::LOADER)]
function to_pgsql_table(Client $client, string $table): PostgreSqlLoader
{
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
): InsertOptions {
    return new InsertOptions($skipConflicts, $conflictColumns, $conflictConstraint, $updateColumns);
}

/**
 * Create update options for PostgreSQL loader.
 *
 * @param list<string> $primaryKeys Columns to use in WHERE clause for matching rows
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_update_options(array $primaryKeys): UpdateOptions
{
    return new UpdateOptions($primaryKeys);
}

/**
 * Create delete options for PostgreSQL loader.
 *
 * @param list<string> $primaryKeys Columns to use in WHERE clause for matching rows
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_delete_options(array $primaryKeys): DeleteOptions
{
    return new DeleteOptions($primaryKeys);
}

/**
 * Convert a Flow Schema into a PostgreSQL table definition.
 *
 * @param string $databaseSchema PostgreSQL schema (namespace) the table belongs to
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function to_pgsql_schema_table(
    Schema $schema,
    string $tableName,
    string $databaseSchema = 'public',
    ?EntryTypesMap $typesMap = null,
): Table {
    return (new SchemaConverter($typesMap))->toPostgreSqlTable($schema, $tableName, $databaseSchema);
}

/**
 * Convert a PostgreSQL table definition into a Flow Schema.
 */
#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_table_to_flow_schema(Table $table, ?EntryTypesMap $typesMap = null): Schema
{
    return (new SchemaConverter($typesMap))->toFlowSchema($table);
}
