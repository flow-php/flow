<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{DiscardMode, ObjectType};
use Flow\PgQuery\QueryBuilder\Utility\{CommentTarget, DiscardType, ExplainFormat, IndexCleanup, LockMode};
use PHPUnit\Framework\TestCase;

final class EnumsTest extends TestCase
{
    public function test_comment_target_values() : void
    {
        self::assertSame(ObjectType::OBJECT_TABLE, CommentTarget::TABLE->value);
        self::assertSame(ObjectType::OBJECT_COLUMN, CommentTarget::COLUMN->value);
        self::assertSame(ObjectType::OBJECT_INDEX, CommentTarget::INDEX->value);
        self::assertSame(ObjectType::OBJECT_SCHEMA, CommentTarget::SCHEMA->value);
        self::assertSame(ObjectType::OBJECT_SEQUENCE, CommentTarget::SEQUENCE->value);
        self::assertSame(ObjectType::OBJECT_VIEW, CommentTarget::VIEW->value);
        self::assertSame(ObjectType::OBJECT_MATVIEW, CommentTarget::MATERIALIZED_VIEW->value);
        self::assertSame(ObjectType::OBJECT_FUNCTION, CommentTarget::FUNCTION->value);
        self::assertSame(ObjectType::OBJECT_PROCEDURE, CommentTarget::PROCEDURE->value);
        self::assertSame(ObjectType::OBJECT_TRIGGER, CommentTarget::TRIGGER->value);
        self::assertSame(ObjectType::OBJECT_TYPE, CommentTarget::TYPE->value);
        self::assertSame(ObjectType::OBJECT_EXTENSION, CommentTarget::EXTENSION->value);
        self::assertSame(ObjectType::OBJECT_ROLE, CommentTarget::ROLE->value);
        self::assertSame(ObjectType::OBJECT_DATABASE, CommentTarget::DATABASE->value);
    }

    public function test_discard_type_values() : void
    {
        self::assertSame(DiscardMode::DISCARD_ALL, DiscardType::ALL->value);
        self::assertSame(DiscardMode::DISCARD_PLANS, DiscardType::PLANS->value);
        self::assertSame(DiscardMode::DISCARD_SEQUENCES, DiscardType::SEQUENCES->value);
        self::assertSame(DiscardMode::DISCARD_TEMP, DiscardType::TEMP->value);
    }

    public function test_explain_format_values() : void
    {
        self::assertSame('text', ExplainFormat::TEXT->value);
        self::assertSame('xml', ExplainFormat::XML->value);
        self::assertSame('json', ExplainFormat::JSON->value);
        self::assertSame('yaml', ExplainFormat::YAML->value);
    }

    public function test_index_cleanup_values() : void
    {
        self::assertSame('auto', IndexCleanup::AUTO->value);
        self::assertSame('on', IndexCleanup::ON->value);
        self::assertSame('off', IndexCleanup::OFF->value);
    }

    public function test_lock_mode_values() : void
    {
        self::assertSame(1, LockMode::ACCESS_SHARE->value);
        self::assertSame(2, LockMode::ROW_SHARE->value);
        self::assertSame(3, LockMode::ROW_EXCLUSIVE->value);
        self::assertSame(4, LockMode::SHARE_UPDATE_EXCLUSIVE->value);
        self::assertSame(5, LockMode::SHARE->value);
        self::assertSame(6, LockMode::SHARE_ROW_EXCLUSIVE->value);
        self::assertSame(7, LockMode::EXCLUSIVE->value);
        self::assertSame(8, LockMode::ACCESS_EXCLUSIVE->value);
    }
}
