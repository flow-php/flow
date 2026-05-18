<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Database;

use Flow\PostgreSql\Protobuf\AST\CreatedbStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Database\CreateDatabaseBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\create;

final class CreateDatabaseBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_database(): void
    {
        static::assertSame('CREATE DATABASE mydb', create()->database('mydb')->toSql());
    }

    public function test_create_database_ast_type(): void
    {
        static::assertInstanceOf(CreatedbStmt::class, CreateDatabaseBuilder::create('mydb')->toAst());
    }

    public function test_create_database_if_not_exists(): void
    {
        static::assertSame('CREATE DATABASE mydb IF_NOT_EXISTS 1', create()->database('mydb')->ifNotExists()->toSql());
    }

    public function test_create_database_if_not_exists_with_options(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb IF_NOT_EXISTS 1 OWNER admin',
            create()->database('mydb')->ifNotExists()->owner('admin')->toSql(),
        );
    }

    public function test_create_database_immutability(): void
    {
        $original = CreateDatabaseBuilder::create('mydb');
        $modified = $original->ifNotExists();

        static::assertSame('CREATE DATABASE mydb', $original->toSql());
        static::assertSame('CREATE DATABASE mydb IF_NOT_EXISTS 1', $modified->toSql());
    }

    public function test_create_database_sets_name_in_ast(): void
    {
        static::assertSame('mydb', CreateDatabaseBuilder::create('mydb')->toAst()->getDbname());
    }

    public function test_create_database_with_all_options(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb OWNER admin TEMPLATE template0 ENCODING "UTF8" LOCALE "en_US.UTF-8" LC_COLLATE "en_US.UTF-8" LC_CTYPE "en_US.UTF-8" CONNECTION LIMIT 10',
            create()
                ->database('mydb')
                ->owner('admin')
                ->template('template0')
                ->encoding('UTF8')
                ->locale('en_US.UTF-8')
                ->lcCollate('en_US.UTF-8')
                ->lcCtype('en_US.UTF-8')
                ->connectionLimit(10)
                ->toSql(),
        );
    }

    public function test_create_database_with_allow_connections(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb ALLOW_CONNECTIONS 0',
            create()->database('mydb')->allowConnections(false)->toSql(),
        );
    }

    public function test_create_database_with_allow_connections_true(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb ALLOW_CONNECTIONS 1',
            create()->database('mydb')->allowConnections(true)->toSql(),
        );
    }

    public function test_create_database_with_builtin_locale(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb BUILTIN_LOCALE "C"',
            create()->database('mydb')->builtinLocale('C')->toSql(),
        );
    }

    public function test_create_database_with_collation_version(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb COLLATION_VERSION "1.0"',
            create()->database('mydb')->collationVersion('1.0')->toSql(),
        );
    }

    public function test_create_database_with_connection_limit(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb CONNECTION LIMIT 100',
            create()->database('mydb')->connectionLimit(100)->toSql(),
        );
    }

    public function test_create_database_with_encoding(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb ENCODING "UTF8"',
            create()->database('mydb')->encoding('UTF8')->toSql(),
        );
    }

    public function test_create_database_with_icu_locale(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb ICU_LOCALE "en-US"',
            create()->database('mydb')->icuLocale('en-US')->toSql(),
        );
    }

    public function test_create_database_with_icu_rules(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb ICU_RULES "&V << w <<< W"',
            create()->database('mydb')->icuRules('&V << w <<< W')->toSql(),
        );
    }

    public function test_create_database_with_is_template(): void
    {
        static::assertSame('CREATE DATABASE mydb IS_TEMPLATE 1', create()->database('mydb')->isTemplate(true)->toSql());
    }

    public function test_create_database_with_locale(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb LOCALE "en_US.UTF-8"',
            create()->database('mydb')->locale('en_US.UTF-8')->toSql(),
        );
    }

    public function test_create_database_with_locale_provider(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb LOCALE_PROVIDER icu',
            create()->database('mydb')->localeProvider('icu')->toSql(),
        );
    }

    public function test_create_database_with_multiple_options(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb OWNER admin ENCODING "UTF8" CONNECTION LIMIT 50',
            create()->database('mydb')->owner('admin')->encoding('UTF8')->connectionLimit(50)->toSql(),
        );
    }

    public function test_create_database_with_oid(): void
    {
        static::assertSame('CREATE DATABASE mydb OID 12345', create()->database('mydb')->oid(12345)->toSql());
    }

    public function test_create_database_with_owner(): void
    {
        static::assertSame('CREATE DATABASE mydb OWNER admin', create()->database('mydb')->owner('admin')->toSql());
    }

    public function test_create_database_with_strategy(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb STRATEGY wal_log',
            create()->database('mydb')->strategy('wal_log')->toSql(),
        );
    }

    public function test_create_database_with_tablespace(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb TABLESPACE fast_ssd',
            create()->database('mydb')->tablespace('fast_ssd')->toSql(),
        );
    }

    public function test_create_database_with_template(): void
    {
        static::assertSame(
            'CREATE DATABASE mydb TEMPLATE template0',
            create()->database('mydb')->template('template0')->toSql(),
        );
    }
}
