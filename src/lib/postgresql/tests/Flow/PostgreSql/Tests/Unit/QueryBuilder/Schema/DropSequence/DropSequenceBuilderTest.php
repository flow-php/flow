<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\DropSequence;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\DropSequence\DropSequenceBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Types\DSL\type_instance_of;

final class DropSequenceBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_multiple_sequences(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq', 'order_id_seq', 'product_id_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_sequence_cascade(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq')->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_sequence_if_exists(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq')->ifExists();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_sequence_if_exists_cascade(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq')->ifExists()->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_sequence_restrict(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq')->restrict();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_sequence_with_schema(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('public.user_id_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        $objects = $ast->getObjects();
        static::assertCount(1, $objects);
        $firstObject = type_instance_of(Node::class)->assert($objects[0]);
        $list = $firstObject->getList();
        static::assertNotNull($list);
        $items = $list->getItems();
        static::assertCount(2, $items);
        $itemFirst = type_instance_of(Node::class)->assert($items[0]);
        static::assertSame('public', $itemFirst->getString()?->getSval());
        $itemSecond = type_instance_of(Node::class)->assert($items[1]);
        static::assertSame('user_id_seq', $itemSecond->getString()?->getSval());
    }

    public function test_immutability(): void
    {
        $original = DropSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->cascade();

        static::assertSame(DropBehavior::DROP_BEHAVIOR_UNDEFINED, $original->toAst()->getBehavior());
        static::assertSame(DropBehavior::DROP_CASCADE, $modified->toAst()->getBehavior());
    }

    public function test_simple_drop_sequence(): void
    {
        $builder = DropSequenceBuilder::create()->sequence('user_id_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getRemoveType());
        $objects = $ast->getObjects();
        static::assertCount(1, $objects);
        $firstObject = type_instance_of(Node::class)->assert($objects[0]);
        $list = $firstObject->getList();
        static::assertNotNull($list);
        $items = $list->getItems();
        $itemFirst = type_instance_of(Node::class)->assert($items[0]);
        static::assertSame('user_id_seq', $itemFirst->getString()?->getSval());
    }
}
