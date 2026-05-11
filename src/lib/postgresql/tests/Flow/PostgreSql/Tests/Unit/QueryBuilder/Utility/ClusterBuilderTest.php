<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\ClusterStmt;
use Flow\PostgreSql\QueryBuilder\Utility\ClusterBuilder;
use PHPUnit\Framework\TestCase;

final class ClusterBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_cluster_all(): void
    {
        $builder = ClusterBuilder::all();

        $ast = $builder->toAst();

        static::assertInstanceOf(ClusterStmt::class, $ast);
        static::assertNull($ast->getRelation());
    }

    public function test_cluster_table(): void
    {
        $builder = ClusterBuilder::create()->table('users');

        $ast = $builder->toAst();

        static::assertNotNull($ast->getRelation());
        static::assertSame('users', $ast->getRelation()->getRelname());
    }

    public function test_cluster_table_using_index(): void
    {
        $builder = ClusterBuilder::create()->table('users')->using('idx_users_pkey');

        $ast = $builder->toAst();

        static::assertSame('users', $ast->getRelation()?->getRelname());
        static::assertSame('idx_users_pkey', $ast->getIndexname());
    }

    public function test_cluster_table_with_schema(): void
    {
        $builder = ClusterBuilder::create()->table('public.users');

        $ast = $builder->toAst();

        static::assertSame('public', $ast->getRelation()?->getSchemaname());
        static::assertSame('users', $ast->getRelation()?->getRelname());
    }

    public function test_cluster_verbose(): void
    {
        $builder = ClusterBuilder::create()->verbose()->table('users');

        $ast = $builder->toAst();

        static::assertCount(1, $ast->getParams());
        static::assertSame('verbose', $ast->getParams()[0]->getDefElem()?->getDefname());
    }

    public function test_immutability(): void
    {
        $original = ClusterBuilder::create();
        $modified = $original->table('users');

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertNull($originalAst->getRelation());
        static::assertNotNull($modifiedAst->getRelation());
    }
}
