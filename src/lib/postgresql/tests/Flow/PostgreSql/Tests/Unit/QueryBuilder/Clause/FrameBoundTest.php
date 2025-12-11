<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\{FrameBound, FrameBoundType};
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class FrameBoundTest extends TestCase
{
    public function test_current_row() : void
    {
        $bound = FrameBound::currentRow();

        self::assertSame(FrameBoundType::CURRENT_ROW, $bound->type());
        self::assertNull($bound->offset());
    }

    public function test_following_with_offset() : void
    {
        $offset = Literal::int(5);
        $bound = FrameBound::following($offset);

        self::assertSame(FrameBoundType::FOLLOWING, $bound->type());
        self::assertSame($offset, $bound->offset());
    }

    public function test_preceding_with_offset() : void
    {
        $offset = Literal::int(10);
        $bound = FrameBound::preceding($offset);

        self::assertSame(FrameBoundType::PRECEDING, $bound->type());
        self::assertSame($offset, $bound->offset());
    }

    public function test_to_ast_and_from_ast_current_row() : void
    {
        $bound = FrameBound::currentRow();
        $node = $bound->toAst();

        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $node);
    }

    public function test_to_ast_with_offset() : void
    {
        $offset = Literal::int(3);
        $bound = FrameBound::preceding($offset);
        $node = $bound->toAst();

        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $node);
    }

    public function test_unbounded_following() : void
    {
        $bound = FrameBound::unboundedFollowing();

        self::assertSame(FrameBoundType::UNBOUNDED_FOLLOWING, $bound->type());
        self::assertNull($bound->offset());
    }

    public function test_unbounded_preceding() : void
    {
        $bound = FrameBound::unboundedPreceding();

        self::assertSame(FrameBoundType::UNBOUNDED_PRECEDING, $bound->type());
        self::assertNull($bound->offset());
    }
}
