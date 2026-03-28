<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateTable;

use Flow\PostgreSql\Protobuf\AST\{CreateStmt, PartitionStrategy};
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, ColumnType};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\{ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint};
use Flow\PostgreSql\QueryBuilder\Schema\CreateTable\CreateTableBuilder;
use PHPUnit\Framework\TestCase;

final class CreateTableBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_table_if_not_exists() : void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->ifNotExists();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertTrue($ast->getIfNotExists());
    }

    public function test_create_table_inherits() : void
    {
        $builder = CreateTableBuilder::create('employees')
            ->column(ColumnDefinition::create('department', ColumnType::text()))
            ->inherits('persons');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertCount(1, $ast->getInhRelations());
        self::assertSame('persons', $ast->getInhRelations()[0]->getRangeVar()->getRelname());
    }

    public function test_create_table_inherits_multiple() : void
    {
        $builder = CreateTableBuilder::create('employees')
            ->column(ColumnDefinition::create('department', ColumnType::text()))
            ->inherits('persons', 'contacts');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertCount(2, $ast->getInhRelations());
    }

    public function test_create_table_partition_by_hash() : void
    {
        $builder = CreateTableBuilder::create('orders')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->partitionByHash('id');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertTrue($ast->hasPartspec());
        self::assertSame(PartitionStrategy::PARTITION_STRATEGY_HASH, $ast->getPartspec()->getStrategy());
    }

    public function test_create_table_partition_by_list() : void
    {
        $builder = CreateTableBuilder::create('sales')
            ->column(ColumnDefinition::create('region', ColumnType::text()))
            ->partitionByList('region');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertTrue($ast->hasPartspec());
        self::assertSame(PartitionStrategy::PARTITION_STRATEGY_LIST, $ast->getPartspec()->getStrategy());
        self::assertCount(1, $ast->getPartspec()->getPartParams());
    }

    public function test_create_table_partition_by_range() : void
    {
        $builder = CreateTableBuilder::create('logs')
            ->column(ColumnDefinition::create('created_at', ColumnType::timestamp()))
            ->partitionByRange('created_at');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertTrue($ast->hasPartspec());
        self::assertSame(PartitionStrategy::PARTITION_STRATEGY_RANGE, $ast->getPartspec()->getStrategy());
    }

    public function test_create_table_tablespace() : void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->tablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertSame('fast_storage', $ast->getTablespacename());
    }

    public function test_create_table_temporary() : void
    {
        $builder = CreateTableBuilder::create('temp_users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->temporary();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertSame('t', $ast->getRelation()->getRelpersistence());
        self::assertSame(0, $ast->getOncommit());
    }

    public function test_create_table_unlogged() : void
    {
        $builder = CreateTableBuilder::create('unlogged_users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->unlogged();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertSame('u', $ast->getRelation()->getRelpersistence());
    }

    public function test_create_table_with_constraint() : void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->column(ColumnDefinition::create('email', ColumnType::varchar(255)))
            ->constraint(PrimaryKeyConstraint::create('id'))
            ->constraint(UniqueConstraint::create('email'));

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertCount(4, $ast->getTableElts());
        self::assertTrue($ast->getTableElts()[2]->hasConstraint());
        self::assertTrue($ast->getTableElts()[3]->hasConstraint());
    }

    public function test_create_table_with_foreign_key_constraint() : void
    {
        $builder = CreateTableBuilder::create('orders')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->column(ColumnDefinition::create('user_id', ColumnType::integer()))
            ->constraint(ForeignKeyConstraint::create(['user_id'], 'users', ['id']));

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertCount(3, $ast->getTableElts());
        self::assertTrue($ast->getTableElts()[2]->hasConstraint());
    }

    public function test_create_table_with_multiple_columns() : void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::serial()))
            ->column(ColumnDefinition::create('name', ColumnType::varchar(100))->notNull())
            ->column(ColumnDefinition::create('email', ColumnType::varchar(255))->unique());

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertCount(3, $ast->getTableElts());
    }

    public function test_create_table_with_schema() : void
    {
        $builder = CreateTableBuilder::create('users', 'public')
            ->column(ColumnDefinition::create('id', ColumnType::integer()));

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertSame('public', $ast->getRelation()->getSchemaname());
        self::assertSame('users', $ast->getRelation()->getRelname());
    }

    public function test_immutability() : void
    {
        $original = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()));
        $modified = $original->ifNotExists();

        self::assertFalse($original->toAst()->getIfNotExists());
        self::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_simple_create_table() : void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()));

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateStmt::class, $ast);
        self::assertSame('users', $ast->getRelation()->getRelname());
        self::assertCount(1, $ast->getTableElts());
        self::assertSame('p', $ast->getRelation()->getRelpersistence());
    }
}
