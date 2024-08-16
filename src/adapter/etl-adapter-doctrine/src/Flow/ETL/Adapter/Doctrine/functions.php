<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type as DbalType;
use Doctrine\DBAL\{ArrayParameterType as DbalArrayType, Connection, ParameterType as DbalParameterType};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Attribute\DocumentationDSL,
    Attribute\Module,
    Attribute\Type as DSLType
};

/**
 * @param array<string, mixed>|Connection $connection
 * @param string $query
 * @param QueryParameter ...$parameters
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::HELPER)]
function dbal_dataframe_factory(
    array|Connection $connection,
    string $query,
    QueryParameter ...$parameters
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
    return DbalLimitOffsetExtractor::table(
        $connection,
        \is_string($table) ? new Table($table) : $table,
        $order_by instanceof OrderBy ? [$order_by] : $order_by,
        $page_size,
        $maximum,
    );
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
    return new DbalLimitOffsetExtractor(
        $connection,
        $queryBuilder,
        $page_size,
        $maximum,
    );
}

/**
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
    return new DbalQueryExtractor(
        $connection,
        $query,
        $parameters_set,
        $types,
    );
}

/**
 * @param array<string, mixed>|list<mixed> $parameters
 * @param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::EXTRACTOR)]
function dbal_from_query(
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
 * In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array<string, mixed>|Connection $connection
 * @param array{
 *  skip_conflicts?: boolean,
 *  constraint?: string,
 *  conflict_columns?: array<string>,
 *  update_columns?: array<string>,
 *  primary_key_columns?: array<string>
 * } $options
 *
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::DOCTRINE, type: DSLType::LOADER)]
function to_dbal_table_insert(
    array|Connection $connection,
    string $table,
    array $options = [],
) : DbalLoader {
    return \is_array($connection)
        ? new DbalLoader($table, $connection, $options, 'insert')
        : DbalLoader::fromConnection($connection, $table, $options, 'insert');
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
 * } $options
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
        ? new DbalLoader($table, $connection, $options, 'update')
        : DbalLoader::fromConnection($connection, $table, $options, 'update');
}
