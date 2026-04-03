<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Database;

use function Flow\PostgreSql\DSL\drop;
use Flow\PostgreSql\Protobuf\AST\DropdbStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Database\DropDatabaseBuilder;
use PHPUnit\Framework\TestCase;

final class DropDatabaseBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_database() : void
    {
        self::assertSame('DROP DATABASE mydb', drop()->database('mydb')->toSql());
    }

    public function test_drop_database_ast_type() : void
    {
        self::assertInstanceOf(DropdbStmt::class, DropDatabaseBuilder::create('mydb')->toAst());
    }

    public function test_drop_database_force() : void
    {
        self::assertSame('DROP DATABASE mydb (FORCE)', drop()->database('mydb')->force()->toSql());
    }

    public function test_drop_database_force_sets_option_in_ast() : void
    {
        $ast = DropDatabaseBuilder::create('mydb')->force()->toAst();

        self::assertCount(1, $ast->getOptions());

        $defElem = $ast->getOptions()[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('force', $defElem->getDefname());
    }

    public function test_drop_database_if_exists() : void
    {
        self::assertSame('DROP DATABASE IF EXISTS mydb', drop()->database('mydb')->ifExists()->toSql());
    }

    public function test_drop_database_if_exists_sets_missing_ok_in_ast() : void
    {
        self::assertTrue(DropDatabaseBuilder::create('mydb')->ifExists()->toAst()->getMissingOk());
    }

    public function test_drop_database_if_exists_with_force() : void
    {
        self::assertSame('DROP DATABASE IF EXISTS mydb (FORCE)', drop()->database('mydb')->ifExists()->force()->toSql());
    }

    public function test_drop_database_immutability() : void
    {
        $original = DropDatabaseBuilder::create('mydb');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_database_immutability_force() : void
    {
        $original = DropDatabaseBuilder::create('mydb');
        $withForce = $original->force();

        self::assertCount(0, $original->toAst()->getOptions());
        self::assertCount(1, $withForce->toAst()->getOptions());
    }

    public function test_drop_database_sets_name_in_ast() : void
    {
        self::assertSame('mydb', DropDatabaseBuilder::create('mydb')->toAst()->getDbname());
    }
}
