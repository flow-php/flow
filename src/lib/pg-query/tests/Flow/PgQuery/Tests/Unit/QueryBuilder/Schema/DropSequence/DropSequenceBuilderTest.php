<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\DropSequence;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, ObjectType};
use Flow\PgQuery\QueryBuilder\Schema\DropSequence\DropSequenceBuilder;
use PHPUnit\Framework\TestCase;

final class DropSequenceBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_multiple_sequences() : void
    {
        $builder = DropSequenceBuilder::create()
            ->sequence('user_id_seq', 'order_id_seq', 'product_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(3, $ast->getObjects());
    }

    public function test_drop_sequence_cascade() : void
    {
        $builder = DropSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_sequence_if_exists() : void
    {
        $builder = DropSequenceBuilder::ifExists()
            ->sequence('user_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_sequence_if_exists_cascade() : void
    {
        $builder = DropSequenceBuilder::ifExists()
            ->sequence('user_id_seq')
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_sequence_restrict() : void
    {
        $builder = DropSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->restrict();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_sequence_with_schema() : void
    {
        $builder = DropSequenceBuilder::create()
            ->sequence('public.user_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(1, $ast->getObjects());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertCount(2, $ast->getObjects()[0]->getList()->getItems() ?? []);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('public', $ast->getObjects()[0]->getList()->getItems()[0]->getString()?->getSval());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('user_id_seq', $ast->getObjects()[0]->getList()->getItems()[1]->getString()?->getSval());
    }

    public function test_immutability() : void
    {
        $original = DropSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->cascade();

        self::assertSame(DropBehavior::DROP_BEHAVIOR_UNDEFINED, $original->toAst()->getBehavior());
        self::assertSame(DropBehavior::DROP_CASCADE, $modified->toAst()->getBehavior());
    }

    public function test_simple_drop_sequence() : void
    {
        $builder = DropSequenceBuilder::create()
            ->sequence('user_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getRemoveType());
        self::assertCount(1, $ast->getObjects());
        self::assertSame('user_id_seq', $ast->getObjects()[0]->getList()?->getItems()[0]->getString()?->getSval());
    }
}
