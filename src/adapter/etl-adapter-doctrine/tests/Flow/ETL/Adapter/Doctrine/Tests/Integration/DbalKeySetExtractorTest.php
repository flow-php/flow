<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb,
    pagination_key_asc,
    pagination_key_set,
    to_dbal_schema_table};
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{int_schema, schema, str_schema};
use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\InvalidArgumentException;

final class DbalKeySetExtractorTest extends IntegrationTestCase
{
    public function test_throws_exception_for_empty_key_set() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('KeySet must contain at least one key for pagination');

        from_dbal_key_set_qb(
            $this->pgsqlDatabaseContext->connection(),
            $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                ->from($table)
                ->select('*'),
            pagination_key_set()
        );
    }

    public function test_throws_exception_for_invalid_maximum() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->pgsqlDatabaseContext->insert($table, ['id' => 1, 'name' => 'name_1']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -1');

        data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'))
                )->withMaximum(-1)
            )
            ->fetch();
    }

    public function test_throws_exception_for_invalid_page_size() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->pgsqlDatabaseContext->insert($table, ['id' => 1, 'name' => 'name_1']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got 0');

        data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'))
                )->withPageSize(0)
            )
            ->fetch();
    }

    public function test_throws_exception_for_pre_existing_order_by_in_query_builder() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Keyset pagination cannot be used with an ORDER BY clause, please remove OrderBy from Query Builder');

        from_dbal_key_set_qb(
            $this->pgsqlDatabaseContext->connection(),
            $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                ->from($table)
                ->select('*')
                ->orderBy('id', 'ASC'),
            pagination_key_set(pagination_key_asc('id'))
        );
    }
}
