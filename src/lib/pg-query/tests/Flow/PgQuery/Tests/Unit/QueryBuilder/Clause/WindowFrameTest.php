<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Clause;

use Flow\PgQuery\QueryBuilder\Clause\{FrameBound, FrameExclusion, FrameMode, WindowFrame};
use PHPUnit\Framework\TestCase;

final class WindowFrameTest extends TestCase
{
    public function test_groups_frame() : void
    {
        $frame = WindowFrame::groups(FrameBound::currentRow());

        self::assertSame(FrameMode::GROUPS, $frame->mode());
        self::assertInstanceOf(FrameBound::class, $frame->startBound());
        self::assertNull($frame->endBound());
    }

    public function test_groups_frame_between() : void
    {
        $frame = WindowFrame::groups(
            FrameBound::unboundedPreceding(),
            FrameBound::currentRow()
        );

        self::assertSame(FrameMode::GROUPS, $frame->mode());
        self::assertInstanceOf(FrameBound::class, $frame->startBound());
        self::assertInstanceOf(FrameBound::class, $frame->endBound());
    }

    public function test_range_frame_between() : void
    {
        $frame = WindowFrame::range(
            FrameBound::unboundedPreceding(),
            FrameBound::currentRow()
        );

        self::assertSame(FrameMode::RANGE, $frame->mode());
        self::assertInstanceOf(FrameBound::class, $frame->startBound());
        self::assertInstanceOf(FrameBound::class, $frame->endBound());
    }

    public function test_range_frame_current_row() : void
    {
        $frame = WindowFrame::range(FrameBound::currentRow());

        self::assertSame(FrameMode::RANGE, $frame->mode());
        self::assertInstanceOf(FrameBound::class, $frame->startBound());
    }

    public function test_rows_frame_current_row() : void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());

        self::assertSame(FrameMode::ROWS, $frame->mode());
        self::assertInstanceOf(FrameBound::class, $frame->startBound());
    }

    public function test_to_ast() : void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $node = $frame->toAst();

        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\Node::class, $node);
    }

    public function test_with_exclusion() : void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $frameWithExclusion = $frame->withExclusion(FrameExclusion::CURRENT_ROW);

        self::assertSame(FrameExclusion::NO_OTHERS, $frame->exclusion());
        self::assertSame(FrameExclusion::CURRENT_ROW, $frameWithExclusion->exclusion());
    }
}
