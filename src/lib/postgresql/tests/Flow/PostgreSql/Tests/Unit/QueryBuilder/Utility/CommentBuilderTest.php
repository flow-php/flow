<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\CommentStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Utility\CommentBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;
use PHPUnit\Framework\TestCase;

final class CommentBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_comment_on_column(): void
    {
        $builder = CommentBuilder::create(CommentTarget::COLUMN, 'users.email')->is('Email column');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_COLUMN, $ast->getObjtype());
        static::assertSame('Email column', $ast->getComment());
        static::assertCount(2, $ast->getObject()?->getList()?->getItems() ?? []);
    }

    public function test_comment_on_extension(): void
    {
        $builder = CommentBuilder::create(CommentTarget::EXTENSION, 'uuid-ossp')->is('Extension comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_EXTENSION, $ast->getObjtype());
        static::assertSame('Extension comment', $ast->getComment());
    }

    public function test_comment_on_function(): void
    {
        $builder = CommentBuilder::create(CommentTarget::FUNCTION, 'get_user')->is('Function comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        static::assertSame('Function comment', $ast->getComment());
    }

    public function test_comment_on_index(): void
    {
        $builder = CommentBuilder::create(CommentTarget::INDEX, 'idx_users_email')->is('Index comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_INDEX, $ast->getObjtype());
        static::assertSame('Index comment', $ast->getComment());
    }

    public function test_comment_on_schema(): void
    {
        $builder = CommentBuilder::create(CommentTarget::SCHEMA, 'public')->is('Default schema');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_SCHEMA, $ast->getObjtype());
        static::assertSame('Default schema', $ast->getComment());
    }

    public function test_comment_on_sequence(): void
    {
        $builder = CommentBuilder::create(CommentTarget::SEQUENCE, 'users_id_seq')->is('Sequence comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjtype());
        static::assertSame('Sequence comment', $ast->getComment());
    }

    public function test_comment_on_table(): void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'users')->is('A comment');

        $ast = $builder->toAst();

        static::assertInstanceOf(CommentStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame('A comment', $ast->getComment());
    }

    public function test_comment_on_table_with_schema(): void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'public.users')->is('Comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame('Comment', $ast->getComment());
        static::assertCount(2, $ast->getObject()?->getList()?->getItems() ?? []);
    }

    public function test_comment_on_type(): void
    {
        $builder = CommentBuilder::create(CommentTarget::TYPE, 'my_type')->is('Type comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_TYPE, $ast->getObjtype());
        static::assertSame('Type comment', $ast->getComment());
    }

    public function test_comment_on_view(): void
    {
        $builder = CommentBuilder::create(CommentTarget::VIEW, 'active_users')->is('View comment');

        $ast = $builder->toAst();

        static::assertSame(ObjectType::OBJECT_VIEW, $ast->getObjtype());
        static::assertSame('View comment', $ast->getComment());
    }

    public function test_comment_remove_null(): void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'users')->isNull();

        $ast = $builder->toAst();

        static::assertSame('', $ast->getComment());
    }

    public function test_immutability(): void
    {
        $original = CommentBuilder::create(CommentTarget::TABLE, 'users');
        $modified = $original->is('Test');

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertSame('', $originalAst->getComment());
        static::assertSame('Test', $modifiedAst->getComment());
    }
}
