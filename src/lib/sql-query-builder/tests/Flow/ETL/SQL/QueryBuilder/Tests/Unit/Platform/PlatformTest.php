<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Unit\Platform;

use Flow\ETL\SQL\QueryBuilder\Platform\GenericPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\MySQLPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\PostgreSQLPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\SQLitePlatform;
use PHPUnit\Framework\TestCase;

class PlatformTest extends TestCase
{
    public function test_generic_platform_cte_support(): void
    {
        $platform = new GenericPlatform();
        
        self::assertTrue($platform->supportsCTE());
        self::assertTrue($platform->supportsRecursiveCTE());
        self::assertTrue($platform->supportsLateralJoins());
        self::assertEquals('generic', $platform->getName());
    }

    public function test_postgresql_platform(): void
    {
        $platform = new PostgreSQLPlatform();
        
        self::assertTrue($platform->supportsCTE());
        self::assertTrue($platform->supportsRecursiveCTE());
        self::assertTrue($platform->supportsLateralJoins());
        self::assertEquals('postgresql', $platform->getName());
    }

    public function test_mysql_platform(): void
    {
        $platform = new MySQLPlatform();
        
        self::assertTrue($platform->supportsCTE());
        self::assertTrue($platform->supportsRecursiveCTE());
        self::assertTrue($platform->supportsLateralJoins());
        self::assertEquals('mysql', $platform->getName());
    }

    public function test_sqlite_platform(): void
    {
        $platform = new SQLitePlatform();
        
        self::assertTrue($platform->supportsCTE());
        self::assertTrue($platform->supportsRecursiveCTE());
        self::assertFalse($platform->supportsLateralJoins());
        self::assertEquals('sqlite', $platform->getName());
    }

    public function test_build_simple_query(): void
    {
        $platform = new GenericPlatform();
        
        $queryParts = [
            'ctes' => [],
            'select' => ['id', 'name'],
            'from' => ['table' => 'users', 'alias' => 'u'],
            'joins' => [],
            'where' => ['u.active = 1'],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [['column' => 'name', 'direction' => 'ASC']],
            'limit' => 10,
            'offset' => null,
        ];
        
        $sql = $platform->buildSQL($queryParts);
        
        $expected = <<<SQL
SELECT id, name
FROM "users" AS "u"
WHERE u.active = 1
ORDER BY "name" ASC
LIMIT 10
SQL;
        
        self::assertEquals($expected, $sql);
    }

    public function test_build_query_with_cte(): void
    {
        $platform = new GenericPlatform();
        
        $queryParts = [
            'ctes' => [
                'active_users' => [
                    'query' => '(SELECT * FROM users WHERE active = 1)',
                    'columns' => ['id', 'name'],
                    'recursive' => false,
                ],
            ],
            'select' => ['*'],
            'from' => ['table' => 'active_users', 'alias' => null],
            'joins' => [],
            'where' => [],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [],
            'limit' => null,
            'offset' => null,
        ];
        
        $sql = $platform->buildSQL($queryParts);
        
        self::assertStringStartsWith('WITH "active_users" ("id", "name") AS (SELECT * FROM users WHERE active = 1)', $sql);
        self::assertStringContainsString('FROM "active_users"', $sql);
    }

    public function test_postgresql_lateral_join(): void
    {
        $platform = new PostgreSQLPlatform();
        
        $queryParts = [
            'ctes' => [],
            'select' => ['u.id', 'recent.*'],
            'from' => ['table' => 'users', 'alias' => 'u'],
            'joins' => [
                [
                    'type' => 'LEFT LATERAL',
                    'fromAlias' => 'u',
                    'table' => '(SELECT * FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 5)',
                    'alias' => 'recent',
                    'condition' => null,
                ],
            ],
            'where' => [],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [],
            'limit' => null,
            'offset' => null,
        ];
        
        $sql = $platform->buildSQL($queryParts);
        
        self::assertStringContainsString('LEFT JOIN LATERAL (SELECT * FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 5) AS "recent" ON TRUE', $sql);
    }

    public function test_mysql_limit_offset_handling(): void
    {
        $platform = new MySQLPlatform();
        
        $queryParts = [
            'ctes' => [],
            'select' => ['*'],
            'from' => ['table' => 'users', 'alias' => null],
            'joins' => [],
            'where' => [],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [],
            'limit' => 10,
            'offset' => 20,
        ];
        
        $sql = $platform->buildSQL($queryParts);
        
        // MySQL uses LIMIT offset, count syntax
        self::assertStringContainsString('LIMIT 20, 10', $sql);
        self::assertStringNotContainsString('OFFSET', $sql);
    }

    public function test_platform_specific_literals(): void
    {
        $platforms = [
            new GenericPlatform(),
            new PostgreSQLPlatform(),
            new MySQLPlatform(),
            new SQLitePlatform(),
        ];
        
        foreach ($platforms as $platform) {
            self::assertEquals('NULL', $platform->literal(null));
            self::assertIsString($platform->literal('test'));
            self::assertIsString($platform->literal(123));
            self::assertIsString($platform->literal(45.67));
        }
        
        // Boolean handling differs
        $genericPlatform = new GenericPlatform();
        self::assertEquals('TRUE', $genericPlatform->literal(true));
        self::assertEquals('FALSE', $genericPlatform->literal(false));
        
        $mysqlPlatform = new MySQLPlatform();
        self::assertEquals('1', $mysqlPlatform->literal(true));
        self::assertEquals('0', $mysqlPlatform->literal(false));
        
        $sqlitePlatform = new SQLitePlatform();
        self::assertEquals('1', $sqlitePlatform->literal(true));
        self::assertEquals('0', $sqlitePlatform->literal(false));
    }

    public function test_quote_identifier_escaping(): void
    {
        $platform = new GenericPlatform();
        
        // Test escaping of quotes in identifiers
        self::assertEquals('"test""quote"', $platform->quoteIdentifier('test"quote'));
        self::assertEquals('"table"."column""name"', $platform->quoteIdentifier('table.column"name'));
    }

    public function test_mysql_quote_identifier(): void
    {
        $platform = new MySQLPlatform();
        
        self::assertEquals('`users`', $platform->quoteIdentifier('users'));
        self::assertEquals('`schema`.`table`', $platform->quoteIdentifier('schema.table'));
        self::assertEquals('`test``quote`', $platform->quoteIdentifier('test`quote'));
    }
}