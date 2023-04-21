<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Doctrine\DbalDataFrameFactory;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\QueryParameter;
use Flow\ETL\Adapter\Doctrine\Table;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;

class Dbal
{
    /**
     * @param array<string, mixed>|Connection $connection
     * @param string $query
     * @param QueryParameter ...$parameters
     *
     * @return DataFrameFactory
     */
    final public static function dataframe_factory(
        array|Connection $connection,
        string $query,
        QueryParameter ...$parameters
    ) : DataFrameFactory {
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
     * @param string $row_entry_name
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_limit_offset(
        Connection $connection,
        string|Table $table,
        array|OrderBy $order_by,
        int $page_size = 1000,
        ?int $maximum = null,
        string $row_entry_name = 'row'
    ) : Extractor {
        return DbalLimitOffsetExtractor::table(
            $connection,
            \is_string($table) ? new Table($table) : $table,
            $order_by instanceof OrderBy ? [$order_by] : $order_by,
            $page_size,
            $maximum,
            $row_entry_name
        );
    }

    /**
     * @param Connection $connection
     * @param int $page_size
     * @param null|int $maximum
     * @param string $row_entry_name
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_limit_offset_qb(
        Connection $connection,
        QueryBuilder $queryBuilder,
        int $page_size = 1000,
        ?int $maximum = null,
        string $row_entry_name = 'row'
    ) : Extractor {
        return new DbalLimitOffsetExtractor(
            $connection,
            $queryBuilder,
            $page_size,
            $maximum,
            $row_entry_name
        );
    }

    /**
     * @param Connection $connection
     * @param string $query
     * @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     * @param string $row_entry_name
     *
     * @return Extractor
     */
    final public static function from_queries(
        Connection $connection,
        string $query,
        ParametersSet $parameters_set = null,
        array $types = [],
        string $row_entry_name = 'row'
    ) : Extractor {
        return new DbalQueryExtractor(
            $connection,
            $query,
            $parameters_set,
            $types,
            $row_entry_name
        );
    }

    /**
     * @param Connection $connection
     * @param string $query
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     * @param string $row_entry_name
     *
     * @return Extractor
     */
    final public static function from_query(
        Connection $connection,
        string $query,
        array $parameters = [],
        array $types = [],
        string $row_entry_name = 'row'
    ) : Extractor {
        return DbalQueryExtractor::single(
            $connection,
            $query,
            $parameters,
            $types,
            $row_entry_name
        );
    }

    /**
     * @param array<string, mixed>|Connection $connection
     * @param string $table
     * @param int $chunk_size
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $options
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_table_insert(
        array|Connection $connection,
        string $table,
        int $chunk_size = 1000,
        array $options = [],
    ) : Loader {
        return \is_array($connection)
            ? new DbalLoader($table, $chunk_size, $connection, $options, 'insert')
            : DbalLoader::fromConnection($connection, $table, $chunk_size, $options, 'insert');
    }

    /**
     * @param array<string, mixed>|Connection $connection
     * @param string $table
     * @param int $chunk_size
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $options
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_table_update(
        array|Connection $connection,
        string $table,
        int $chunk_size = 1000,
        array $options = [],
    ) : Loader {
        return \is_array($connection)
            ? new DbalLoader($table, $chunk_size, $connection, $options, 'update')
            : DbalLoader::fromConnection($connection, $table, $chunk_size, $options, 'update');
    }
}
