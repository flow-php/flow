<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\Index\DropIndex\DropIndexBuilder;
use PHPUnit\Framework\TestCase;

final class DropIndexBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_cascade_sets_behavior(): void
    {
        $builder = DropIndexBuilder::create('idx_users_email')->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_concurrently_sets_flag(): void
    {
        $builder = DropIndexBuilder::create('idx_users_email')->concurrently();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getConcurrent());
    }

    public function test_drop_multiple_indexes(): void
    {
        $builder = DropIndexBuilder::create('idx_a', 'idx_b', 'idx_c');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertCount(3, $ast->getObjects());
    }

    public function test_if_exists_sets_flag(): void
    {
        $builder = DropIndexBuilder::create('idx_users_email')->ifExists();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_immutability(): void
    {
        $original = DropIndexBuilder::create('idx_users_email');
        $modified = $original->ifExists();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertFalse($originalAst->getMissingOk());
        static::assertTrue($modifiedAst->getMissingOk());
    }

    public function test_restrict_sets_behavior(): void
    {
        $builder = DropIndexBuilder::create('idx_users_email')->restrict();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_simple_drop_index(): void
    {
        $builder = DropIndexBuilder::create('idx_users_email');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_INDEX, $ast->getRemoveType());
        static::assertCount(1, $ast->getObjects());
    }

    public function test_with_schema(): void
    {
        $builder = DropIndexBuilder::create('public.idx_users_email');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertCount(1, $ast->getObjects());
        static::assertCount(2, $ast->getObjects()[0]->getList()->getItems());
        static::assertSame('public', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
        static::assertSame('idx_users_email', $ast->getObjects()[0]->getList()->getItems()[1]->getString()->getSval());
    }
}
