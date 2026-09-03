<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use DateInterval;
use DateTimeImmutable;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\MysqliResultColumns;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\Tests\Context\MysqlSessionStatus;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\SchemaNotDerivableException;
use mysqli;
use mysqli_driver;

use function array_keys;
use function Flow\ETL\Adapter\Doctrine\from_dbal_key_set_qb;
use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class MySQLSchemaDerivationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqlDatabaseContext->createTable(
            (new Table('orders', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('amount', Type::getType(Types::DECIMAL), [
                    'notnull' => true,
                    'precision' => 10,
                    'scale' => 2,
                ]),
                new Column('placed_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => false]),
                new Column('opens_at', Type::getType(Types::TIME_MUTABLE), ['notnull' => false]),
            ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()),
        );

        for ($i = 1; $i <= 3; $i++) {
            $this->mysqlDatabaseContext->insert('orders', [
                'id' => $i,
                'name' => 'name_' . $i,
                'amount' => $i * 10.5,
                'placed_at' => '2025-01-0' . $i . ' 12:00:00',
                'opens_at' => '0' . $i . ':30:00',
            ]);
        }
    }

    public function test_duplicate_output_names_describe_and_collapse_last_wins(): void
    {
        // MySQL rejects a derived table whose columns repeat a name, so wrapping this query would
        // refuse a read that works. The raw prepare keeps both columns and the schema collapses them
        // exactly as fetchAssociative() collapses the row.
        $connection = $this->mysqlDatabaseContext->connection();
        $connection->executeStatement('DROP TABLE IF EXISTS order_lines');
        $connection->executeStatement('CREATE TABLE order_lines (id INT NOT NULL, label VARCHAR(255))');
        $connection->executeStatement("INSERT INTO order_lines VALUES (1, 'first')");

        try {
            $extractor = from_dbal_query($connection, 'SELECT * FROM orders o JOIN order_lines l ON l.id = o.id');

            static::assertSame(
                ['id', 'name', 'amount', 'placed_at', 'opens_at', 'label'],
                $extractor->schema()->references()->names(),
            );
            static::assertSame(
                ['id', 'name', 'amount', 'placed_at', 'opens_at', 'label'],
                array_keys(data_frame()->read($extractor)->fetch()->toArray()[0]),
            );
        } finally {
            $connection->executeStatement('DROP TABLE order_lines');
        }
    }

    public function test_a_parameterised_query_describes(): void
    {
        $extractor = from_dbal_query(
            $this->mysqlDatabaseContext->connection(),
            'SELECT id, name FROM orders WHERE id > :min',
            ['min' => 1],
        );

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());
        static::assertCount(2, data_frame()->read($extractor)->fetch());
    }

    public function test_a_time_column_hydrates(): void
    {
        $rows = data_frame()
            ->read(from_dbal_query(
                $this->mysqlDatabaseContext->connection(),
                'SELECT opens_at FROM orders WHERE id = 2',
            ))
            ->fetch()
            ->toArray();

        static::assertInstanceOf(DateInterval::class, $rows[0]['opens_at']);
        static::assertSame('02:30:00', $rows[0]['opens_at']->format('%H:%I:%S'));
    }

    public function test_a_trailing_semicolon_query_still_describes(): void
    {
        static::assertSame(
            ['id'],
            from_dbal_query($this->mysqlDatabaseContext->connection(), 'SELECT id FROM orders;')
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_a_view_describes(): void
    {
        $connection = $this->mysqlDatabaseContext->connection();
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
                $this->mysqlDatabaseContext->connection(),
                'SELECT id, name AS bb, amount * 2 AS calc FROM orders',
            )->schema(),
        );
    }

    public function test_reads_inside_from_all(): void
    {
        $rows = data_frame()
            ->read(from_all(
                from_dbal_query(
                    $this->mysqlDatabaseContext->connection(),
                    'SELECT id, name, amount, placed_at FROM orders ORDER BY id',
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
            $this->mysqlDatabaseContext->connection(),
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
            from_dbal_query(
                $this->mysqlDatabaseContext->connection(),
                'SELECT id, name, amount, placed_at FROM orders',
            )->schema(),
        );
    }

    public function test_schema_describes_the_base_builder(): void
    {
        $connection = $this->mysqlDatabaseContext->connection();
        $extractor = from_dbal_key_set_qb(
            $connection,
            $connection->createQueryBuilder()->select('id', 'name')->from('orders'),
            pagination_key_set(pagination_key_asc('id')),
        );

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());

        foreach ($extractor->extract(flow_context()) as $rows) {
            static::assertSame(['id', 'name'], $rows->schema()->references()->names());
        }
    }

    public function test_the_type_probe_executes_no_query(): void
    {
        $native = $this->mysqlDatabaseContext->connection()->getNativeConnection();
        static::assertInstanceOf(mysqli::class, $native);
        $status = new MysqlSessionStatus($native);

        $preparedBefore = $status->of('Com_stmt_prepare');
        $executedBefore = $status->of('Com_stmt_execute');

        from_dbal_query($this->mysqlDatabaseContext->connection(), 'SELECT id, name FROM orders')->schema();

        // One prepare, zero executes: describing a MySQL result costs no query at all, because
        // result_metadata() answers off the prepared statement and there is no DBAL name probe.
        static::assertSame(1, $status->of('Com_stmt_prepare') - $preparedBefore);
        static::assertSame(0, $status->of('Com_stmt_execute') - $executedBefore);
    }

    public function test_the_native_type_probe_refuses_when_the_driver_reports_no_exception(): void
    {
        // DBAL never sets the report mode, so an application that turned exceptions off gets false
        // back from prepare() instead of a throw; the refusal must read the same either way.
        $native = $this->mysqlDatabaseContext->connection()->getNativeConnection();
        static::assertInstanceOf(mysqli::class, $native);

        $reporting = (new mysqli_driver())->report_mode;
        mysqli_report(MYSQLI_REPORT_OFF);

        try {
            (new MysqliResultColumns())->of($native, 'SELECT id FROM missing', self::class);
            static::fail('a query the database refuses must not describe');
        } catch (SchemaNotDerivableException $e) {
            static::assertStringContainsString('MySQL refused to prepare this query', $e->getMessage());
            static::assertStringContainsString('missing', $e->getMessage());
        } finally {
            mysqli_report($reporting);
        }
    }

    public function test_the_native_type_probe_refuses_with_the_driver_message(): void
    {
        $native = $this->mysqlDatabaseContext->connection()->getNativeConnection();
        static::assertInstanceOf(mysqli::class, $native);

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessageMatches('/MySQL refused to prepare this query \(.*missing.*doesn\'t exist/');

        (new MysqliResultColumns())->of($native, 'SELECT id FROM missing', self::class);
    }
}
