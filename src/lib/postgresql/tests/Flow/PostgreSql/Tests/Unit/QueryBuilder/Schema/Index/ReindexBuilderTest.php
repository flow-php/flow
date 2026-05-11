<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\ReindexObjectType;
use Flow\PostgreSql\Protobuf\AST\ReindexStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex\ReindexBuilder;
use PHPUnit\Framework\TestCase;

final class ReindexBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_concurrently_adds_param(): void
    {
        $builder = ReindexBuilder::index('idx_users_email')->concurrently();

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertCount(1, $ast->getParams());
        static::assertSame('concurrently', $ast->getParams()[0]->getDefElem()->getDefname());
    }

    public function test_database_reindex(): void
    {
        $builder = ReindexBuilder::database('mydb');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame(ReindexObjectType::REINDEX_OBJECT_DATABASE, $ast->getKind());
        static::assertSame('mydb', $ast->getName());
    }

    public function test_immutability(): void
    {
        $original = ReindexBuilder::index('idx_users_email');
        $modified = $original->concurrently();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertCount(0, $originalAst->getParams());
        static::assertCount(1, $modifiedAst->getParams());
    }

    public function test_index_reindex(): void
    {
        $builder = ReindexBuilder::index('idx_users_email');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame(ReindexObjectType::REINDEX_OBJECT_INDEX, $ast->getKind());
        static::assertSame('idx_users_email', $ast->getRelation()->getRelname());
    }

    public function test_index_with_schema(): void
    {
        $builder = ReindexBuilder::index('public.idx_users_email');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame('public', $ast->getRelation()->getSchemaname());
        static::assertSame('idx_users_email', $ast->getRelation()->getRelname());
    }

    public function test_multiple_params(): void
    {
        $builder = ReindexBuilder::table('users')->concurrently()->verbose()->tablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertCount(3, $ast->getParams());
    }

    public function test_schema_reindex(): void
    {
        $builder = ReindexBuilder::schema('public');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame(ReindexObjectType::REINDEX_OBJECT_SCHEMA, $ast->getKind());
        static::assertSame('public', $ast->getName());
    }

    public function test_system_reindex(): void
    {
        $builder = ReindexBuilder::system('mydb');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame(ReindexObjectType::REINDEX_OBJECT_SYSTEM, $ast->getKind());
        static::assertSame('mydb', $ast->getName());
    }

    public function test_table_reindex(): void
    {
        $builder = ReindexBuilder::table('users');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertSame(ReindexObjectType::REINDEX_OBJECT_TABLE, $ast->getKind());
        static::assertSame('users', $ast->getRelation()->getRelname());
    }

    public function test_tablespace_adds_param(): void
    {
        $builder = ReindexBuilder::index('idx_users_email')->tablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertCount(1, $ast->getParams());
        static::assertSame('tablespace', $ast->getParams()[0]->getDefElem()->getDefname());
        static::assertSame('fast_storage', $ast->getParams()[0]->getDefElem()->getArg()->getString()->getSval());
    }

    public function test_verbose_adds_param(): void
    {
        $builder = ReindexBuilder::table('users')->verbose();

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        static::assertCount(1, $ast->getParams());
        static::assertSame('verbose', $ast->getParams()[0]->getDefElem()->getDefname());
    }
}
