<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Flow\ETL\Adapter\Doctrine\DbalResultSchema;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Double\NativeHandleStub;
use Flow\ETL\Adapter\Doctrine\Tests\Double\NonSqlitePdo;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;
use stdClass;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DbalResultSchemaTest extends FlowTestCase
{
    public function test_a_missing_sqlite_column_rethrows_what_the_read_throws(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('no such column: nope');

        (new DbalResultSchema())->of(
            InMemorySqlite::withUsers(InMemorySqlite::connection(), 1),
            'SELECT nope FROM users',
            self::class,
        );
    }

    public function test_a_missing_table_rethrows_what_the_read_throws(): void
    {
        $this->expectException(TableNotFoundException::class);

        (new DbalResultSchema())->of(InMemorySqlite::connection(), 'SELECT id FROM missing', self::class);
    }

    public function test_a_query_the_probe_cannot_wrap_chains_the_driver_error(): void
    {
        try {
            (new DbalResultSchema())->of(InMemorySqlite::connection(), 'SELECT 1; SELECT 2', self::class);
            static::fail('a query the probe cannot wrap must not describe');
        } catch (SchemaNotDerivableException $e) {
            static::assertInstanceOf(SyntaxErrorException::class, $e->getPrevious());
            static::assertStringContainsString('If the query runs as written', $e->getMessage());
        }
    }

    public function test_a_query_that_binds_parameters_describes(): void
    {
        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true)),
            (new DbalResultSchema())->of(
                InMemorySqlite::withUsers(InMemorySqlite::connection(), 3),
                'SELECT id, name FROM users WHERE id > :min',
                self::class,
            ),
        );
    }

    public function test_a_pdo_connection_that_is_not_sqlite_is_refused(): void
    {
        // pdo_pgsql and pdo_mysql hand out a plain PDO with no arm; describing them as all-string
        // would be a wrong schema, which is worse than none.
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('reports no column types for a query result, so this query cannot be typed');

        (new DbalResultSchema())->of(
            InMemorySqlite::withUsers(InMemorySqlite::connection(new NativeHandleStub(new NonSqlitePdo())), 1),
            'SELECT id FROM users',
            self::class,
        );
    }

    public function test_an_unsupported_driver_is_refused_by_name(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            'the stdClass driver reports no column types for a query result, so this query cannot be typed',
        );

        (new DbalResultSchema())->of(
            InMemorySqlite::withUsers(InMemorySqlite::connection(new NativeHandleStub(new stdClass())), 1),
            'SELECT id FROM users',
            self::class,
        );
    }

    public function test_the_wrapper_is_applied_so_a_trailing_semicolon_still_describes(): void
    {
        static::assertSame(
            ['id'],
            (new DbalResultSchema())
                ->of(InMemorySqlite::withUsers(InMemorySqlite::connection(), 1), 'SELECT id FROM users;', self::class)
                ->references()
                ->names(),
        );
    }
}
