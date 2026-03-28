<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateTableAs;

use function Flow\PostgreSql\DSL\{col, literal};
use Flow\PostgreSql\Protobuf\AST\{CreateTableAsStmt, ObjectType};
use Flow\PostgreSql\QueryBuilder\Schema\CreateTableAs\CreateTableAsBuilder;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;

use PHPUnit\Framework\TestCase;

final class CreateTableAsBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_table_as_if_not_exists() : void
    {
        $select = SelectBuilder::create()
            ->select(literal(1));

        $builder = CreateTableAsBuilder::create('new_table', $select)
            ->ifNotExists();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertTrue($ast->getIfNotExists());
    }

    public function test_create_table_as_with_column_names() : void
    {
        $select = SelectBuilder::create()
            ->select(col('id'), col('name'));

        $builder = CreateTableAsBuilder::create('new_table', $select)
            ->columnNames('user_id', 'user_name');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertCount(2, $ast->getInto()->getColNames());
        self::assertSame('user_id', $ast->getInto()->getColNames()[0]->getString()->getSval());
        self::assertSame('user_name', $ast->getInto()->getColNames()[1]->getString()->getSval());
    }

    public function test_create_table_as_with_no_data() : void
    {
        $select = SelectBuilder::create()
            ->select(col('id'), col('name'))
            ->from(new Table('users'));

        $builder = CreateTableAsBuilder::create('users_copy', $select)
            ->withNoData();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertTrue($ast->getInto()->getSkipData());
    }

    public function test_create_table_as_with_schema() : void
    {
        $select = SelectBuilder::create()
            ->select(col('id'), col('email'))
            ->from(new Table('users'));

        $builder = CreateTableAsBuilder::create('new_table', $select, 'archive');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertSame('archive', $ast->getInto()->getRel()->getSchemaname());
        self::assertSame('new_table', $ast->getInto()->getRel()->getRelname());
    }

    public function test_immutability() : void
    {
        $select = SelectBuilder::create()
            ->select(literal(1));

        $original = CreateTableAsBuilder::create('new_table', $select);
        $modified = $original->ifNotExists();

        self::assertFalse($original->toAst()->getIfNotExists());
        self::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_simple_create_table_as() : void
    {
        $select = SelectBuilder::create()
            ->select(col('id'), col('name'))
            ->from(new Table('users'));

        $builder = CreateTableAsBuilder::create('users_copy', $select);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertSame('users_copy', $ast->getInto()->getRel()->getRelname());
        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        self::assertTrue($ast->hasQuery());
    }

    public function test_with_all_options() : void
    {
        $select = SelectBuilder::create()
            ->select(col('id'), col('name'))
            ->from(new Table('users'));

        $builder = CreateTableAsBuilder::create('users_backup', $select, 'archive')
            ->columnNames('user_id', 'user_name')
            ->ifNotExists()
            ->withNoData();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertSame('archive', $ast->getInto()->getRel()->getSchemaname());
        self::assertSame('users_backup', $ast->getInto()->getRel()->getRelname());
        self::assertCount(2, $ast->getInto()->getColNames());
        self::assertTrue($ast->getIfNotExists());
        self::assertTrue($ast->getInto()->getSkipData());
    }
}
