<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardMode;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use Flow\PostgreSql\QueryBuilder\Utility\IndexCleanup;
use Flow\PostgreSql\QueryBuilder\Utility\LockMode;
use PHPUnit\Framework\TestCase;

final class EnumsTest extends TestCase
{
    public function test_comment_target_values(): void
    {
        static::assertSame(ObjectType::OBJECT_TABLE, CommentTarget::TABLE->value);
        static::assertSame(ObjectType::OBJECT_COLUMN, CommentTarget::COLUMN->value);
        static::assertSame(ObjectType::OBJECT_INDEX, CommentTarget::INDEX->value);
        static::assertSame(ObjectType::OBJECT_SCHEMA, CommentTarget::SCHEMA->value);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, CommentTarget::SEQUENCE->value);
        static::assertSame(ObjectType::OBJECT_VIEW, CommentTarget::VIEW->value);
        static::assertSame(ObjectType::OBJECT_MATVIEW, CommentTarget::MATERIALIZED_VIEW->value);
        static::assertSame(ObjectType::OBJECT_FUNCTION, CommentTarget::FUNCTION->value);
        static::assertSame(ObjectType::OBJECT_PROCEDURE, CommentTarget::PROCEDURE->value);
        static::assertSame(ObjectType::OBJECT_TRIGGER, CommentTarget::TRIGGER->value);
        static::assertSame(ObjectType::OBJECT_TYPE, CommentTarget::TYPE->value);
        static::assertSame(ObjectType::OBJECT_EXTENSION, CommentTarget::EXTENSION->value);
        static::assertSame(ObjectType::OBJECT_ROLE, CommentTarget::ROLE->value);
        static::assertSame(ObjectType::OBJECT_DATABASE, CommentTarget::DATABASE->value);
    }

    public function test_discard_type_values(): void
    {
        static::assertSame(DiscardMode::DISCARD_ALL, DiscardType::ALL->value);
        static::assertSame(DiscardMode::DISCARD_PLANS, DiscardType::PLANS->value);
        static::assertSame(DiscardMode::DISCARD_SEQUENCES, DiscardType::SEQUENCES->value);
        static::assertSame(DiscardMode::DISCARD_TEMP, DiscardType::TEMP->value);
    }

    public function test_explain_format_values(): void
    {
        static::assertSame('text', ExplainFormat::TEXT->value);
        static::assertSame('xml', ExplainFormat::XML->value);
        static::assertSame('json', ExplainFormat::JSON->value);
        static::assertSame('yaml', ExplainFormat::YAML->value);
    }

    public function test_index_cleanup_values(): void
    {
        static::assertSame('auto', IndexCleanup::AUTO->value);
        static::assertSame('on', IndexCleanup::ON->value);
        static::assertSame('off', IndexCleanup::OFF->value);
    }

    public function test_lock_mode_values(): void
    {
        static::assertSame(1, LockMode::ACCESS_SHARE->value);
        static::assertSame(2, LockMode::ROW_SHARE->value);
        static::assertSame(3, LockMode::ROW_EXCLUSIVE->value);
        static::assertSame(4, LockMode::SHARE_UPDATE_EXCLUSIVE->value);
        static::assertSame(5, LockMode::SHARE->value);
        static::assertSame(6, LockMode::SHARE_ROW_EXCLUSIVE->value);
        static::assertSame(7, LockMode::EXCLUSIVE->value);
        static::assertSame(8, LockMode::ACCESS_EXCLUSIVE->value);
    }
}
