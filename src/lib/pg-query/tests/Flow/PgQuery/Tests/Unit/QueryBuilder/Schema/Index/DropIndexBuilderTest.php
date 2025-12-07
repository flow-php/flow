<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, ObjectType};
use Flow\PgQuery\QueryBuilder\Schema\Index\DropIndex\DropIndexBuilder;
use PHPUnit\Framework\TestCase;

final class DropIndexBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_cascade_sets_behavior() : void
    {
        $builder = DropIndexBuilder::create('idx_users_email')
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_concurrently_sets_flag() : void
    {
        $builder = DropIndexBuilder::create('idx_users_email')
            ->concurrently();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getConcurrent());
    }

    public function test_drop_multiple_indexes() : void
    {
        $builder = DropIndexBuilder::create('idx_a', 'idx_b', 'idx_c');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(3, $ast->getObjects());
    }

    public function test_if_exists_sets_flag() : void
    {
        $builder = DropIndexBuilder::create('idx_users_email')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
    }

    public function test_immutability() : void
    {
        $original = DropIndexBuilder::create('idx_users_email');
        $modified = $original->ifExists();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertFalse($originalAst->getMissingOk());
        self::assertTrue($modifiedAst->getMissingOk());
    }

    public function test_restrict_sets_behavior() : void
    {
        $builder = DropIndexBuilder::create('idx_users_email')
            ->restrict();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_simple_drop_index() : void
    {
        $builder = DropIndexBuilder::create('idx_users_email');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_INDEX, $ast->getRemoveType());
        self::assertCount(1, $ast->getObjects());
    }

    public function test_with_schema() : void
    {
        $builder = DropIndexBuilder::create('public.idx_users_email');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(1, $ast->getObjects());
        self::assertCount(2, $ast->getObjects()[0]->getList()->getItems());
        self::assertSame('public', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
        self::assertSame('idx_users_email', $ast->getObjects()[0]->getList()->getItems()[1]->getString()->getSval());
    }
}
