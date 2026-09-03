<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use DateTimeImmutable;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DescribeQuery;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\PgSqlResultColumns;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\SchemaNotDerivableException;
use PgSql\Connection as PgSqlConnection;

use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PostgreSQLSchemaDerivationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->pgsqlDatabaseContext->createTable(
            (new Table('orders', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('amount', Type::getType(Types::DECIMAL), [
                    'notnull' => true,
                    'precision' => 10,
                    'scale' => 2,
                ]),
                new Column('placed_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => false]),
            ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()),
        );

        for ($i = 1; $i <= 3; $i++) {
            $this->pgsqlDatabaseContext->insert('orders', [
                'id' => $i,
                'name' => 'name_' . $i,
                'amount' => $i * 10.5,
                'placed_at' => '2025-01-0' . $i . ' 12:00:00',
            ]);
        }
    }

    public function test_a_parameterised_query_describes(): void
    {
        $extractor = from_dbal_query(
            $this->pgsqlDatabaseContext->connection(),
            'SELECT id, name FROM orders WHERE id > :min',
            ['min' => 1],
        );

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());
        static::assertCount(2, data_frame()->read($extractor)->fetch());
    }

    public function test_the_probe_never_binds_the_callers_values(): void
    {
        // 'not-an-integer' would make PostgreSQL refuse this query outright. Describing it succeeds,
        // which is only possible because the probe binds null at every position instead.
        static::assertSame(
            ['id', 'name'],
            from_dbal_query($this->pgsqlDatabaseContext->connection(), 'SELECT id, name FROM orders WHERE id > :min', [
                'min' => 'not-an-integer',
            ])->schema()->references()->names(),
        );
    }

    public function test_a_trailing_semicolon_query_still_describes(): void
    {
        static::assertSame(
            ['id'],
            from_dbal_query($this->pgsqlDatabaseContext->connection(), 'SELECT id FROM orders;')
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_a_view_describes(): void
    {
        $connection = $this->pgsqlDatabaseContext->connection();
        $connection->executeStatement('DROP VIEW IF EXISTS orders_v');
        $connection->executeStatement('CREATE VIEW orders_v AS SELECT id, name FROM orders');

        try {
            static::assertEquals(
                schema(int_schema('id', true), str_schema('name', true)),
                from_dbal_limit_offset($connection, 'orders_v', new OrderBy('id', Order::ASC))->schema(),
            );
        } finally {
            $connection->executeStatement('DROP VIEW orders_v');
        }
    }

    public function test_an_aliased_and_computed_select_list_describes(): void
    {
        static::assertEquals(
            schema(int_schema('id', true), str_schema('bb', true), float_schema('calc', true)),
            from_dbal_query(
                $this->pgsqlDatabaseContext->connection(),
                'SELECT id, name AS bb, amount * 2 AS calc FROM orders',
            )->schema(),
        );
    }

    public function test_an_unmapped_column_type_refuses_the_whole_read(): void
    {
        $connection = $this->pgsqlDatabaseContext->connection();
        $connection->executeStatement('DROP TABLE IF EXISTS priced');
        $connection->executeStatement('CREATE TABLE priced (id INT NOT NULL, price MONEY)');

        try {
            $extractor = from_dbal_query($connection, 'SELECT id, price FROM priced');

            try {
                $extractor->schema();
                static::fail('money has no Flow type on this route');
            } catch (SchemaNotDerivableException $e) {
                static::assertStringContainsString(
                    'column "price" has driver type "money", which Flow has no type for',
                    $e->getMessage(),
                );
            }

            $batches = 0;

            try {
                foreach ($extractor->extract(flow_context()) as $_rows) {
                    $batches++;
                }
                static::fail('a read that cannot be described must not run');
            } catch (SchemaNotDerivableException $e) {
                static::assertStringContainsString('column "price" has driver type "money"', $e->getMessage());
            }

            static::assertSame(0, $batches);
        } finally {
            $connection->executeStatement('DROP TABLE priced');
        }
    }

    public function test_numeric_arrives_as_float(): void
    {
        static::assertSame(
            [['calc' => 21.0]],
            data_frame()
                ->read(from_dbal_query(
                    $this->pgsqlDatabaseContext->connection(),
                    'SELECT amount * 2 AS calc FROM orders WHERE id = 1',
                ))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_reads_inside_from_all(): void
    {
        $rows = data_frame()
            ->read(from_all(
                from_dbal_limit_offset(
                    $this->pgsqlDatabaseContext->connection(),
                    'orders',
                    new OrderBy('id', Order::ASC),
                ),
                from_array([[
                    'id' => 99,
                    'name' => 'name_99',
                    'amount' => 1.5,
                    'placed_at' => new DateTimeImmutable('2025-02-01 00:00:00'),
                ]]),
            ))
            ->fetch();

        static::assertCount(4, $rows);
        static::assertSame([1, 2, 3, 99], $rows->reduceToArray('id'));
    }

    public function test_schema_and_extract_agree(): void
    {
        $extractor = from_dbal_limit_offset(
            $this->pgsqlDatabaseContext->connection(),
            'orders',
            new OrderBy('id', Order::ASC),
            2,
        );

        $batches = 0;

        foreach ($extractor->extract(flow_context()) as $rows) {
            $batches++;
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
        }

        static::assertSame(3, $batches);
    }

    public function test_schema_comes_from_result_metadata(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', true),
                str_schema('name', true),
                float_schema('amount', true),
                datetime_schema('placed_at', true),
            ),
            from_dbal_limit_offset(
                $this->pgsqlDatabaseContext->connection(),
                'orders',
                new OrderBy('id', Order::ASC),
            )->schema(),
        );
    }

    public function test_the_derived_schema_is_memoised(): void
    {
        $extractor = from_dbal_limit_offset(
            $this->pgsqlDatabaseContext->connection(),
            'orders',
            new OrderBy('id', Order::ASC),
        );

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();
        $extractor->schema();
        $afterFirst = $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries();
        $extractor->schema();

        // Relative, not absolute: the second call must add nothing to what the first cost.
        static::assertSame($afterFirst, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
    }

    public function test_the_native_type_probe_refuses_with_the_driver_message(): void
    {
        $native = $this->pgsqlDatabaseContext->connection()->getNativeConnection();
        static::assertInstanceOf(PgSqlConnection::class, $native);

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessageMatches(
            '/PostgreSQL refused the zero-row probe of this query \(.*relation "missing" does not exist/',
        );

        (new PgSqlResultColumns())->of($native, (new DescribeQuery())->of('SELECT id FROM missing'), self::class);
    }
}
