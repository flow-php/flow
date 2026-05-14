<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ReindexObjectType;
use Flow\PostgreSql\Protobuf\AST\ReindexStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex\ReindexBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

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
        $params = $ast->getParams();
        static::assertCount(1, $params);
        $defElem = type_instance_of(Node::class)->assert($params[0])->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('concurrently', $defElem->getDefname());
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
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('idx_users_email', $relation->getRelname());
    }

    public function test_index_with_schema(): void
    {
        $builder = ReindexBuilder::index('public.idx_users_email');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('public', $relation->getSchemaname());
        static::assertSame('idx_users_email', $relation->getRelname());
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
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
    }

    public function test_tablespace_adds_param(): void
    {
        $builder = ReindexBuilder::index('idx_users_email')->tablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        $params = $ast->getParams();
        static::assertCount(1, $params);
        $defElem = type_instance_of(Node::class)->assert($params[0])->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('tablespace', $defElem->getDefname());
        $arg = $defElem->getArg();
        static::assertNotNull($arg);
        $string = $arg->getString();
        static::assertNotNull($string);
        static::assertSame('fast_storage', $string->getSval());
    }

    public function test_verbose_adds_param(): void
    {
        $builder = ReindexBuilder::table('users')->verbose();

        $ast = $builder->toAst();

        static::assertInstanceOf(ReindexStmt::class, $ast);
        $params = $ast->getParams();
        static::assertCount(1, $params);
        $defElem = type_instance_of(Node::class)->assert($params[0])->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('verbose', $defElem->getDefname());
    }
}
