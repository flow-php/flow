<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type as DbalType;
use Doctrine\DBAL\{ArrayParameterType as DbalArrayType, Connection, ParameterType as DbalParameterType};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\DocumentationExample, Attribute\Module, Attribute\Type as DSLType};

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
 * @param null|int $maximum
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function from_dbal_limit_offset_qb(
    Connection $connection,
    QueryBuilder $queryBuilder,
    int $page_size = 1000,
    ?int $maximum = null,
) : DbalLimitOffsetExtractor {
    $loader = (new DbalLimitOffsetExtractor(
        $connection,
        $queryBuilder,
    ))->withPageSize($page_size);

    if ($maximum !== null) {
        $loader->withMaximum($maximum);
    }

    return $loader;
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
 * @param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
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
 * @param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
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
 * In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 * @param array{
 *  skip_conflicts?: boolean,
 *  constraint?: string,
 *  conflict_columns?: array<string>,
 *  update_columns?: array<string>,
 *  primary_key_columns?: array<string>
 * } $options - @deprecated use DbalLoader::withOperationOptions() instead
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'data_writing', example: 'database_upsert')]
function to_dbal_table_insert(
    array|Connection $connection,
    string $table,
    array $options = [],
) : DbalLoader {
    return \is_array($connection)
        ? (new DbalLoader($table, $connection))->withOperationOptions($options)
        : DbalLoader::fromConnection($connection, $table, $options);
}

/**
 *  In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 * @param array{
 *  skip_conflicts?: boolean,
 *  constraint?: string,
 *  conflict_columns?: array<string>,
 *  update_columns?: array<string>,
 *  primary_key_columns?: array<string>
 * } $options - @deprecated use DbalLoader::withOperationOptions() instead
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
function to_dbal_table_update(
    array|Connection $connection,
    string $table,
    array $options = [],
) : DbalLoader {
    return \is_array($connection)
        ? (new DbalLoader($table, $connection))->withOperation('update')->withOperationOptions($options)
        : DbalLoader::fromConnection($connection, $table, $options, 'update');
}
