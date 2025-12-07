<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\ClusterStmt;
use Flow\PgQuery\QueryBuilder\Utility\ClusterBuilder;
use PHPUnit\Framework\TestCase;

final class ClusterBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_cluster_all() : void
    {
        $builder = ClusterBuilder::all();

        $ast = $builder->toAst();

        self::assertInstanceOf(ClusterStmt::class, $ast);
        self::assertNull($ast->getRelation());
    }

    public function test_cluster_table() : void
    {
        $builder = ClusterBuilder::create()->table('users');

        $ast = $builder->toAst();

        self::assertNotNull($ast->getRelation());
        self::assertSame('users', $ast->getRelation()->getRelname());
    }

    public function test_cluster_table_using_index() : void
    {
        $builder = ClusterBuilder::create()->table('users')->using('idx_users_pkey');

        $ast = $builder->toAst();

        self::assertSame('users', $ast->getRelation()?->getRelname());
        self::assertSame('idx_users_pkey', $ast->getIndexname());
    }

    public function test_cluster_table_with_schema() : void
    {
        $builder = ClusterBuilder::create()->table('public.users');

        $ast = $builder->toAst();

        self::assertSame('public', $ast->getRelation()?->getSchemaname());
        self::assertSame('users', $ast->getRelation()?->getRelname());
    }

    public function test_cluster_verbose() : void
    {
        $builder = ClusterBuilder::create()->verbose()->table('users');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getParams());
        self::assertSame('verbose', $ast->getParams()[0]->getDefElem()?->getDefname());
    }

    public function test_immutability() : void
    {
        $original = ClusterBuilder::create();
        $modified = $original->table('users');

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertNull($originalAst->getRelation());
        self::assertNotNull($modifiedAst->getRelation());
    }
}
