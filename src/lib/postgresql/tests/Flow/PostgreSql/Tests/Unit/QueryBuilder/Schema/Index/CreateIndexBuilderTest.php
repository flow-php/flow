<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\IndexStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Index\CreateIndex\CreateIndexBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\{IndexColumn, IndexMethod};
use PHPUnit\Framework\TestCase;

final class CreateIndexBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_columns_accepts_index_column_objects() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns(IndexColumn::column('email')->desc(), IndexColumn::column('name')->asc());

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertCount(2, $ast->getIndexParams());
    }

    public function test_columns_accepts_strings() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email', 'name');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertCount(2, $ast->getIndexParams());
    }

    public function test_concurrently_sets_flag() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->concurrently()
            ->on('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertTrue($ast->getConcurrent());
    }

    public function test_if_not_exists_sets_flag() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->ifNotExists()
            ->on('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertTrue($ast->getIfNotExists());
    }

    public function test_immutability() : void
    {
        $original = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email');
        $modified = $original->tablespace('fast_storage');

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertEmpty($originalAst->getTableSpace());
        self::assertSame('fast_storage', $modifiedAst->getTableSpace());
    }

    public function test_include_columns() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email')
            ->include('name', 'created_at');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertCount(2, $ast->getIndexIncludingParams());
    }

    public function test_nulls_not_distinct_sets_flag() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->unique()
            ->on('users')
            ->columns('email')
            ->nullsNotDistinct();

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertTrue($ast->getNullsNotDistinct());
    }

    public function test_on_only_sets_inh_to_false() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->onOnly('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertFalse($ast->getRelation()->getInh());
    }

    public function test_on_sets_inh_to_true() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertTrue($ast->getRelation()->getInh());
    }

    public function test_simple_create_index() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertSame('idx_users_email', $ast->getIdxname());
        self::assertSame('users', $ast->getRelation()->getRelname());
        self::assertCount(1, $ast->getIndexParams());
    }

    public function test_tablespace() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->columns('email')
            ->tablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertSame('fast_storage', $ast->getTableSpace());
    }

    public function test_unique_sets_flag() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->unique()
            ->on('users')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertTrue($ast->getUnique());
    }

    public function test_using_sets_access_method() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users')
            ->using(IndexMethod::HASH)
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertSame('hash', $ast->getAccessMethod());
    }

    public function test_with_schema() : void
    {
        $builder = CreateIndexBuilder::create('idx_users_email')
            ->on('users', 'public')
            ->columns('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(IndexStmt::class, $ast);
        self::assertSame('public', $ast->getRelation()->getSchemaname());
        self::assertSame('users', $ast->getRelation()->getRelname());
    }
}
