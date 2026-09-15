<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Exception;
use Flow\ETL\Adapter\Doctrine\ProbeRefusal;
use Flow\ETL\Adapter\Doctrine\SqliteResultSchema;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Tests\FlowTestCase;
use PDOException;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_string;

final class SqliteResultSchemaTest extends FlowTestCase
{
    public function test_a_native_sqlite3_handle_describes(): void
    {
        $sqlite = InMemorySqlite::sqlite3();
        $sqlite->exec('CREATE TABLE users (id INTEGER, name TEXT, amount REAL)');

        static::assertEquals(
            schema(str_schema('id', true), str_schema('bb', true), str_schema('calc', true)),
            (new SqliteResultSchema())->of(
                $sqlite,
                'SELECT id, name AS bb, amount * 2 AS calc FROM users WHERE id > :min',
                self::class,
            ),
        );
    }

    public function test_a_parameterised_query_still_describes(): void
    {
        static::assertSame(
            ['id', 'name'],
            (new SqliteResultSchema())
                ->of(InMemorySqlite::pdo(users: 1), 'SELECT id, name FROM users WHERE id > :min', self::class)
                ->references()
                ->names(),
        );
    }

    public function test_a_query_the_database_refuses_is_refused_with_its_message(): void
    {
        try {
            (new SqliteResultSchema())->of(InMemorySqlite::pdo(), 'SELECT id FROM missing', self::class);
            static::fail('a query the database refuses must not describe');
        } catch (ProbeRefusal $e) {
            static::assertMatchesRegularExpression(
                '/SQLite refused the zero-row probe of this query \(.*no such table: missing/',
                $e->getMessage(),
            );
            static::assertInstanceOf(PDOException::class, $e->getPrevious());
        }
    }

    public function test_a_query_the_native_sqlite3_refuses_is_refused_with_its_message(): void
    {
        try {
            (new SqliteResultSchema())->of(InMemorySqlite::sqlite3(), 'SELECT id FROM missing', self::class);
            static::fail('a query the database refuses must not describe');
        } catch (ProbeRefusal $e) {
            static::assertMatchesRegularExpression(
                '/SQLite refused the zero-row probe of this query \(.*no such table: missing/',
                $e->getMessage(),
            );
            static::assertInstanceOf(Exception::class, $e->getPrevious());
        }
    }

    public function test_every_column_is_named_and_typed_string(): void
    {
        $pdo = InMemorySqlite::pdo(users: 1);

        $schema = (new SqliteResultSchema())->of(
            $pdo,
            'SELECT o.id, o.name AS bb, o.amount * 2 AS calc FROM users o LEFT JOIN users x ON x.id = 99',
            self::class,
        );

        static::assertSame(['id', 'bb', 'calc'], $schema->references()->names());

        foreach ($schema->definitions() as $definition) {
            static::assertTrue($definition->isNullable());
            static::assertEquals(type_string(), $definition->type());
        }
    }
}
