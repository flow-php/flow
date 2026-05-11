<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\FrameBound;
use Flow\PostgreSql\QueryBuilder\Clause\FrameBoundType;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class FrameBoundTest extends TestCase
{
    public function test_current_row(): void
    {
        $bound = FrameBound::currentRow();

        static::assertSame(FrameBoundType::CURRENT_ROW, $bound->type());
        static::assertNull($bound->offset());
    }

    public function test_following_with_offset(): void
    {
        $offset = Literal::int(5);
        $bound = FrameBound::following($offset);

        static::assertSame(FrameBoundType::FOLLOWING, $bound->type());
        static::assertSame($offset, $bound->offset());
    }

    public function test_preceding_with_offset(): void
    {
        $offset = Literal::int(10);
        $bound = FrameBound::preceding($offset);

        static::assertSame(FrameBoundType::PRECEDING, $bound->type());
        static::assertSame($offset, $bound->offset());
    }

    public function test_to_ast_and_from_ast_current_row(): void
    {
        $bound = FrameBound::currentRow();
        $node = $bound->toAst();

        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $node);
    }

    public function test_to_ast_with_offset(): void
    {
        $offset = Literal::int(3);
        $bound = FrameBound::preceding($offset);
        $node = $bound->toAst();

        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $node);
    }

    public function test_unbounded_following(): void
    {
        $bound = FrameBound::unboundedFollowing();

        static::assertSame(FrameBoundType::UNBOUNDED_FOLLOWING, $bound->type());
        static::assertNull($bound->offset());
    }

    public function test_unbounded_preceding(): void
    {
        $bound = FrameBound::unboundedPreceding();

        static::assertSame(FrameBoundType::UNBOUNDED_PRECEDING, $bound->type());
        static::assertNull($bound->offset());
    }
}
