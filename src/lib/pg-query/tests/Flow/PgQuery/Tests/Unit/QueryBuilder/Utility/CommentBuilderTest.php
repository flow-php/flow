<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{CommentStmt, ObjectType};
use Flow\PgQuery\QueryBuilder\Utility\{CommentBuilder, CommentTarget};
use PHPUnit\Framework\TestCase;

final class CommentBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_comment_on_column() : void
    {
        $builder = CommentBuilder::create(CommentTarget::COLUMN, 'users.email')->is('Email column');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_COLUMN, $ast->getObjtype());
        self::assertSame('Email column', $ast->getComment());
        self::assertCount(2, $ast->getObject()?->getList()?->getItems() ?? []);
    }

    public function test_comment_on_extension() : void
    {
        $builder = CommentBuilder::create(CommentTarget::EXTENSION, 'uuid-ossp')->is('Extension comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_EXTENSION, $ast->getObjtype());
        self::assertSame('Extension comment', $ast->getComment());
    }

    public function test_comment_on_function() : void
    {
        $builder = CommentBuilder::create(CommentTarget::FUNCTION, 'get_user')->is('Function comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        self::assertSame('Function comment', $ast->getComment());
    }

    public function test_comment_on_index() : void
    {
        $builder = CommentBuilder::create(CommentTarget::INDEX, 'idx_users_email')->is('Index comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_INDEX, $ast->getObjtype());
        self::assertSame('Index comment', $ast->getComment());
    }

    public function test_comment_on_schema() : void
    {
        $builder = CommentBuilder::create(CommentTarget::SCHEMA, 'public')->is('Default schema');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_SCHEMA, $ast->getObjtype());
        self::assertSame('Default schema', $ast->getComment());
    }

    public function test_comment_on_sequence() : void
    {
        $builder = CommentBuilder::create(CommentTarget::SEQUENCE, 'users_id_seq')->is('Sequence comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjtype());
        self::assertSame('Sequence comment', $ast->getComment());
    }

    public function test_comment_on_table() : void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'users')->is('A comment');

        $ast = $builder->toAst();

        self::assertInstanceOf(CommentStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        self::assertSame('A comment', $ast->getComment());
    }

    public function test_comment_on_table_with_schema() : void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'public.users')->is('Comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        self::assertSame('Comment', $ast->getComment());
        self::assertCount(2, $ast->getObject()?->getList()?->getItems() ?? []);
    }

    public function test_comment_on_type() : void
    {
        $builder = CommentBuilder::create(CommentTarget::TYPE, 'my_type')->is('Type comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_TYPE, $ast->getObjtype());
        self::assertSame('Type comment', $ast->getComment());
    }

    public function test_comment_on_view() : void
    {
        $builder = CommentBuilder::create(CommentTarget::VIEW, 'active_users')->is('View comment');

        $ast = $builder->toAst();

        self::assertSame(ObjectType::OBJECT_VIEW, $ast->getObjtype());
        self::assertSame('View comment', $ast->getComment());
    }

    public function test_comment_remove_null() : void
    {
        $builder = CommentBuilder::create(CommentTarget::TABLE, 'users')->isNull();

        $ast = $builder->toAst();

        self::assertSame('', $ast->getComment());
    }

    public function test_immutability() : void
    {
        $original = CommentBuilder::create(CommentTarget::TABLE, 'users');
        $modified = $original->is('Test');

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertSame('', $originalAst->getComment());
        self::assertSame('Test', $modifiedAst->getComment());
    }
}
