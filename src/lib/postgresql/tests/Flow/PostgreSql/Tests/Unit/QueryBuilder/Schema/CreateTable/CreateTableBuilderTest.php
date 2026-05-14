<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateTable;

use Flow\PostgreSql\Protobuf\AST\CreateStmt;
use Flow\PostgreSql\Protobuf\AST\PartitionStrategy;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\CreateTable\CreateTableBuilder;
use PHPUnit\Framework\TestCase;

final class CreateTableBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_table_if_not_exists(): void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->ifNotExists();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertTrue($ast->getIfNotExists());
    }

    public function test_create_table_inherits(): void
    {
        $builder = CreateTableBuilder::create('employees')
            ->column(ColumnDefinition::create('department', ColumnType::text()))
            ->inherits('persons');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertCount(1, $ast->getInhRelations());
        $rangeVar = $ast->getInhRelations()[0]->getRangeVar();
        static::assertNotNull($rangeVar);
        static::assertSame('persons', $rangeVar->getRelname());
    }

    public function test_create_table_inherits_multiple(): void
    {
        $builder = CreateTableBuilder::create('employees')
            ->column(ColumnDefinition::create('department', ColumnType::text()))
            ->inherits('persons', 'contacts');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertCount(2, $ast->getInhRelations());
    }

    public function test_create_table_partition_by_hash(): void
    {
        $builder = CreateTableBuilder::create('orders')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->partitionByHash('id');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertTrue($ast->hasPartspec());
        $partspec = $ast->getPartspec();
        static::assertNotNull($partspec);
        static::assertSame(PartitionStrategy::PARTITION_STRATEGY_HASH, $partspec->getStrategy());
    }

    public function test_create_table_partition_by_list(): void
    {
        $builder = CreateTableBuilder::create('sales')
            ->column(ColumnDefinition::create('region', ColumnType::text()))
            ->partitionByList('region');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertTrue($ast->hasPartspec());
        $partspec = $ast->getPartspec();
        static::assertNotNull($partspec);
        static::assertSame(PartitionStrategy::PARTITION_STRATEGY_LIST, $partspec->getStrategy());
        static::assertCount(1, $partspec->getPartParams());
    }

    public function test_create_table_partition_by_range(): void
    {
        $builder = CreateTableBuilder::create('logs')
            ->column(ColumnDefinition::create('created_at', ColumnType::timestamp()))
            ->partitionByRange('created_at');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertTrue($ast->hasPartspec());
        $partspec = $ast->getPartspec();
        static::assertNotNull($partspec);
        static::assertSame(PartitionStrategy::PARTITION_STRATEGY_RANGE, $partspec->getStrategy());
    }

    public function test_create_table_tablespace(): void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->tablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertSame('fast_storage', $ast->getTablespacename());
    }

    public function test_create_table_temporary(): void
    {
        $builder = CreateTableBuilder::create('temp_users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->temporary();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('t', $relation->getRelpersistence());
        static::assertSame(0, $ast->getOncommit());
    }

    public function test_create_table_unlogged(): void
    {
        $builder = CreateTableBuilder::create('unlogged_users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->unlogged();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('u', $relation->getRelpersistence());
    }

    public function test_create_table_with_constraint(): void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->column(ColumnDefinition::create('email', ColumnType::varchar(255)))
            ->constraint(PrimaryKeyConstraint::create('id'))
            ->constraint(UniqueConstraint::create('email'));

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertCount(4, $ast->getTableElts());
        static::assertTrue($ast->getTableElts()[2]->hasConstraint());
        static::assertTrue($ast->getTableElts()[3]->hasConstraint());
    }

    public function test_create_table_with_foreign_key_constraint(): void
    {
        $builder = CreateTableBuilder::create('orders')
            ->column(ColumnDefinition::create('id', ColumnType::integer()))
            ->column(ColumnDefinition::create('user_id', ColumnType::integer()))
            ->constraint(ForeignKeyConstraint::create(['user_id'], 'users', ['id']));

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertCount(3, $ast->getTableElts());
        static::assertTrue($ast->getTableElts()[2]->hasConstraint());
    }

    public function test_create_table_with_multiple_columns(): void
    {
        $builder = CreateTableBuilder::create('users')
            ->column(ColumnDefinition::create('id', ColumnType::serial()))
            ->column(ColumnDefinition::create('name', ColumnType::varchar(100))->notNull())
            ->column(ColumnDefinition::create('email', ColumnType::varchar(255))->unique());

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        static::assertCount(3, $ast->getTableElts());
    }

    public function test_create_table_with_schema(): void
    {
        $builder = CreateTableBuilder::create('users', 'public')->column(ColumnDefinition::create(
            'id',
            ColumnType::integer(),
        ));

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('public', $relation->getSchemaname());
        static::assertSame('users', $relation->getRelname());
    }

    public function test_immutability(): void
    {
        $original = CreateTableBuilder::create('users')->column(ColumnDefinition::create('id', ColumnType::integer()));
        $modified = $original->ifNotExists();

        static::assertFalse($original->toAst()->getIfNotExists());
        static::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_simple_create_table(): void
    {
        $builder = CreateTableBuilder::create('users')->column(ColumnDefinition::create('id', ColumnType::integer()));

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertCount(1, $ast->getTableElts());
        static::assertSame('p', $relation->getRelpersistence());
    }
}
