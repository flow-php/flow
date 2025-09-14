<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\{ArrayParameterType as DbalArrayType,
    Connection,
    ParameterType,
    ParameterType as DbalParameterType,
    Types\Type};
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type as DbalType;
use Flow\Doctrine\Bulk\{Dialect\MySQLInsertOptions,
    Dialect\PostgreSQLInsertOptions,
    Dialect\PostgreSQLUpdateOptions,
    Dialect\SqliteInsertOptions,
    InsertOptions,
    UpdateOptions};
use Flow\ETL\{Adapter\Doctrine\Pagination\Key,
    Adapter\Doctrine\Pagination\KeySet,
    Adapter\Doctrine\Pagination\Order,
    Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType,
    Loader,
    Schema};
use Flow\ETL\Exception\InvalidArgumentException;

/**
 * @param array<string, mixed>|Connection $connection
 * @param string $query
 * @param QueryParameter ...$parameters
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function dbal_dataframe_factory(
    array|Connection $connection,
    string $query,
    QueryParameter ...$parameters,
) : DbalDataFrameFactory {
    return \is_array($connection)
        ? new DbalDataFrameFactory($connection, $query, ...$parameters)
        : DbalDataFrameFactory::fromConnection($connection, $query, ...$parameters);
}

/**
 * @param Connection $connection
 * @param string|Table $table
 * @param array<OrderBy>|OrderBy $order_by
 * @param int $page_size
 * @param null|int $maximum
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_limit_offset(
    Connection $connection,
    string|Table $table,
    array|OrderBy $order_by,
    int $page_size = 1000,
    ?int $maximum = null,
) : DbalLimitOffsetExtractor {
    $loader = (DbalLimitOffsetExtractor::table(
        $connection,
        \is_string($table) ? new Table($table) : $table,
        $order_by instanceof OrderBy ? [$order_by] : $order_by,
    ))->withPageSize($page_size);

    if ($maximum !== null) {
        $loader->withMaximum($maximum);
    }

    return $loader;
}

/**
 * @param Connection $connection
 * @param int $page_size
 * @param null|int $maximum - maximum can also be taken from a query builder, $maximum however is used regardless of the query builder if it's set
 * @param int $offset - offset can also be taken from a query builder, $offset however is used regardless of the query builder if it's set to non 0 value
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_limit_offset_qb(
    Connection $connection,
    QueryBuilder $queryBuilder,
    int $page_size = 1000,
    ?int $maximum = null,
    int $offset = 0,
) : DbalLimitOffsetExtractor {
    $loader = (new DbalLimitOffsetExtractor(
        $connection,
        $queryBuilder,
    ))->withPageSize($page_size)
        ->withOffset($offset);

    if ($maximum !== null) {
        $loader->withMaximum($maximum);
    }

    return $loader;
}

#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_key_set_qb(
    Connection $connection,
    QueryBuilder $queryBuilder,
    KeySet $key_set,
) : DbalKeySetExtractor {
    return new DbalKeySetExtractor($connection, $queryBuilder, $key_set);
}

/**
 * @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query
 * @param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_queries(
    Connection $connection,
    string $query,
    ?ParametersSet $parameters_set = null,
    array $types = [],
) : DbalQueryExtractor {
    $extractor = new DbalQueryExtractor(
        $connection,
        $query
    );

    if ($parameters_set !== null) {
        $extractor->withParameters($parameters_set);
    }

    if ($types !== []) {
        /** @phpstan-ignore-next-line */
        $extractor->withTypes($types);
    }

    return $extractor;
}

/**
 * @deprecated use from_dbal_queries() instead
 *
 * @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query
 * @param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function dbal_from_queries(
    Connection $connection,
    string $query,
    ?ParametersSet $parameters_set = null,
    array $types = [],
) : DbalQueryExtractor {
    return from_dbal_queries($connection, $query, $parameters_set, $types);
}

/**
 * @param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead
 * @param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_query(
    Connection $connection,
    string $query,
    array $parameters = [],
    array $types = [],
) : DbalQueryExtractor {
    return DbalQueryExtractor::single(
        $connection,
        $query,
        $parameters,
        $types,
    );
}

/**
 * @deprecated use from_dbal_query() instead
 *
 * @param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead
 * @param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function dbal_from_query(
    Connection $connection,
    string $query,
    array $parameters = [],
    array $types = [],
) : DbalQueryExtractor {
    return from_dbal_query($connection, $query, $parameters, $types);
}

/**
 * Insert new rows into a database table.
 * Insert can also be used as an upsert with the help of InsertOptions.
 * InsertOptions are platform specific, so please choose the right one for your database.
 *
 *  - MySQLInsertOptions
 *  - PostgreSQLInsertOptions
 *  - SqliteInsertOptions
 *
 * In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'data_writing', example: 'database_upsert')]
function to_dbal_table_insert(
    array|Connection $connection,
    string $table,
    ?InsertOptions $options = null,
) : DbalLoader {
    return \is_array($connection)
        ? (new DbalLoader($table, $connection))->withOperationOptions($options)
        : DbalLoader::fromConnection($connection, $table, $options);
}

/**
 *  Update existing rows in database.
 *
 *  In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
function to_dbal_table_update(
    array|Connection $connection,
    string $table,
    ?UpdateOptions $options = null,
) : DbalLoader {
    return \is_array($connection)
        ? (new DbalLoader($table, $connection))->withOperation('update')->withOperationOptions($options)
        : DbalLoader::fromConnection($connection, $table, $options, 'update');
}

/**
 * Delete rows from database table based on the provided data.
 *
 * In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
function to_dbal_table_delete(
    array|Connection $connection,
    string $table,
) : DbalLoader {
    return $connection instanceof Connection
        ? DbalLoader::fromConnection($connection, $table, null, 'delete')
        : (new DbalLoader($table, $connection))->withOperation('delete');
}

/**
 * Converts a Flow\ETL\Schema to a Doctrine\DBAL\Schema\Table.
 *
 * @param Schema $schema
 * @param array<array-key, mixed> $table_options
 * @param array<class-string<\Flow\Types\Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $types_map
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function to_dbal_schema_table(Schema $schema, string $table_name, array $table_options = [], array $types_map = []) : \Doctrine\DBAL\Schema\Table
{
    return (new SchemaConverter($types_map))->toDbalTable($schema, $table_name, $table_options);
}

/**
 * Converts a Doctrine\DBAL\Schema\Table to a Flow\ETL\Schema.
 *
 * @param array<class-string<\Flow\Types\Type<mixed>>, class-string<\Doctrine\DBAL\Types\Type>> $types_map
 *
 * @return Schema
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function table_schema_to_flow_schema(\Doctrine\DBAL\Schema\Table $table, array $types_map = []) : Schema
{
    return (new SchemaConverter($types_map))->toFlowSchema($table);
}

/**
 * @param array<string> $conflict_columns
 * @param array<string> $update_columns
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
#[DocumentationExample(topic: 'data_writing', example: 'database_upsert')]
function postgresql_insert_options(?bool $skip_conflicts = null, ?string $constraint = null, array $conflict_columns = [], array $update_columns = []) : PostgreSQLInsertOptions
{
    return new PostgreSQLInsertOptions($skip_conflicts, $constraint, $conflict_columns, $update_columns);
}

/**
 * @param array<string> $update_columns
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function mysql_insert_options(?bool $skip_conflicts = null, ?bool $upsert = null, array $update_columns = []) : MySQLInsertOptions
{
    return new MySQLInsertOptions($skip_conflicts, $upsert, $update_columns);
}

/**
 * @param array<string> $conflict_columns
 * @param array<string> $update_columns
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function sqlite_insert_options(?bool $skip_conflicts = null, array $conflict_columns = [], array $update_columns = []) : SqliteInsertOptions
{
    return new SqliteInsertOptions($skip_conflicts, $conflict_columns, $update_columns);
}

/**
 * @param array<string> $primary_key_columns
 * @param array<string> $update_columns
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function postgresql_update_options(
    array $primary_key_columns = [],
    array $update_columns = [],
) : PostgreSQLUpdateOptions {
    return new PostgreSQLUpdateOptions(
        $primary_key_columns,
        $update_columns,
    );
}

/**
 * Execute multiple loaders within a database transaction.
 * Each batch of rows will be processed in its own transaction.
 * If any loader fails, the entire batch will be rolled back.
 *
 * @param array<string, mixed>|Connection $connection
 * @param DbalLoader ...$loaders - DBAL loaders to execute within the transaction
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
function to_dbal_transaction(
    array|Connection $connection,
    DbalLoader ...$loaders,
) : TransactionalDbalLoader {
    return \is_array($connection)
        ? new TransactionalDbalLoader($connection, ...$loaders)
        : TransactionalDbalLoader::fromConnection($connection, ...$loaders);
}

#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function pagination_key_asc(string $column, string|int|ParameterType|Type $type = ParameterType::STRING) : Key
{
    return new Key($column, Order::ASC, $type);
}

#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function pagination_key_desc(string $column, string|int|ParameterType|Type $type = ParameterType::STRING) : Key
{
    return new Key($column, Order::DESC, $type);
}

#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function pagination_key_set(Key ...$keys) : KeySet
{
    return new KeySet(...$keys);
}
