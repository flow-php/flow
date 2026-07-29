<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use Flow\ETL\Tests\Double\CountingWindowFrame;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Tests\Mother\WindowContextMother;
use Flow\ETL\Window\RowsFrame;

use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\preceding;

final class WindowContextTest extends FlowTestCase
{
    public function test_accessors_return_what_was_passed_in(): void
    {
        $partition = RowsMother::sequentialIds(5);
        $context = WindowContextMother::atIndex($partition, 2);

        static::assertSame(2, $context->index());
        static::assertSame($partition, $context->partition());
        static::assertTrue($partition[2]->isEqual($context->row()));
    }

    public function test_empty_bounds_produce_an_empty_frame(): void
    {
        static::assertCount(
            0,
            WindowContextMother::atIndex(
                RowsMother::sequentialIds(5),
                0,
                new RowsFrame(preceding(10), preceding(5)),
            )->frame(),
        );
    }

    public function test_frame_is_resolved_only_once(): void
    {
        $frame = new CountingWindowFrame([0, 2]);
        $context = WindowContextMother::atIndex(RowsMother::sequentialIds(5), 4, $frame);

        $context->frame();
        $context->frame();
        $context->frame();

        static::assertSame(1, $frame->calls);
    }

    public function test_frame_is_never_resolved_when_it_is_not_read(): void
    {
        $frame = new CountingWindowFrame();

        WindowContextMother::atIndex(RowsMother::sequentialIds(5), 4, $frame)->index();

        static::assertSame(0, $frame->calls);
    }

    public function test_frame_is_sliced_according_to_the_window_frame(): void
    {
        $frame = WindowContextMother::atIndex(
            RowsMother::sequentialIds(5),
            3,
            new RowsFrame(preceding(2), current_row()),
        )->frame();

        static::assertCount(3, $frame);
        static::assertSame(2, $frame[0]->valueOf('id'));
        static::assertSame(3, $frame[1]->valueOf('id'));
        static::assertSame(4, $frame[2]->valueOf('id'));
    }
}
