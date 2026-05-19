<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Clause\FrameBound;
use Flow\PostgreSql\QueryBuilder\Clause\FrameExclusion;
use Flow\PostgreSql\QueryBuilder\Clause\FrameMode;
use Flow\PostgreSql\QueryBuilder\Clause\WindowFrame;
use PHPUnit\Framework\TestCase;

final class WindowFrameTest extends TestCase
{
    public function test_groups_frame(): void
    {
        $frame = WindowFrame::groups(FrameBound::currentRow());

        static::assertSame(FrameMode::GROUPS, $frame->mode());
        static::assertInstanceOf(FrameBound::class, $frame->startBound());
        static::assertNull($frame->endBound());
    }

    public function test_groups_frame_between(): void
    {
        $frame = WindowFrame::groups(FrameBound::unboundedPreceding(), FrameBound::currentRow());

        static::assertSame(FrameMode::GROUPS, $frame->mode());
        static::assertInstanceOf(FrameBound::class, $frame->startBound());
        static::assertInstanceOf(FrameBound::class, $frame->endBound());
    }

    public function test_range_frame_between(): void
    {
        $frame = WindowFrame::range(FrameBound::unboundedPreceding(), FrameBound::currentRow());

        static::assertSame(FrameMode::RANGE, $frame->mode());
        static::assertInstanceOf(FrameBound::class, $frame->startBound());
        static::assertInstanceOf(FrameBound::class, $frame->endBound());
    }

    public function test_range_frame_current_row(): void
    {
        $frame = WindowFrame::range(FrameBound::currentRow());

        static::assertSame(FrameMode::RANGE, $frame->mode());
        static::assertInstanceOf(FrameBound::class, $frame->startBound());
    }

    public function test_rows_frame_current_row(): void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());

        static::assertSame(FrameMode::ROWS, $frame->mode());
        static::assertInstanceOf(FrameBound::class, $frame->startBound());
    }

    public function test_to_ast(): void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $node = $frame->toAst();

        static::assertInstanceOf(Node::class, $node);
    }

    public function test_with_exclusion(): void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $frameWithExclusion = $frame->withExclusion(FrameExclusion::CURRENT_ROW);

        static::assertSame(FrameExclusion::NO_OTHERS, $frame->exclusion());
        static::assertSame(FrameExclusion::CURRENT_ROW, $frameWithExclusion->exclusion());
    }
}
