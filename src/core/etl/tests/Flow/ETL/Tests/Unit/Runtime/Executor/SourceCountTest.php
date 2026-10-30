<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Executor\SourceCount;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\FlowTestCase;

final class SourceCountTest extends FlowTestCase
{
    public function test_a_source_never_opened_counts_nothing_and_is_complete(): void
    {
        $read = new SourceCount();

        static::assertSame(0, $read->rows());
        static::assertTrue($read->isComplete());
    }

    public function test_an_open_read_is_not_complete_until_it_closes(): void
    {
        $read = new SourceCount();
        $read->opened(false);
        $read->counted(3);

        static::assertFalse($read->isComplete());

        $read->closed();

        static::assertSame(3, $read->rows());
        static::assertTrue($read->isComplete());
    }

    public function test_every_read_must_close(): void
    {
        $read = new SourceCount();
        $read->opened(false);
        $read->opened(false);
        $read->closed();

        static::assertFalse($read->isComplete());
    }

    public function test_a_narrowed_read_never_makes_the_source_complete(): void
    {
        $read = new SourceCount();
        $read->opened(true);
        $read->closed();
        $read->opened(false);
        $read->closed();

        static::assertFalse($read->isComplete());
    }

    public function test_counts_add_up_across_reads(): void
    {
        $read = new SourceCount();
        $read->counted(2);
        $read->counted(5);

        static::assertSame(7, $read->rows());
    }

    public function test_absorb_takes_over_the_rows_the_open_reads_and_the_narrowing(): void
    {
        $read = new SourceCount();
        $closed = new SourceCount();
        $open = new SourceCount();
        $narrowed = new SourceCount();
        $closed->opened(false);
        $closed->counted(2);
        $closed->closed();
        $open->opened(false);
        $open->counted(3);
        $narrowed->opened(true);
        $narrowed->closed();

        $read->absorb($closed);

        static::assertSame(2, $read->rows());
        static::assertTrue($read->isComplete());

        $read->absorb($open);

        static::assertSame(5, $read->rows());
        static::assertFalse($read->isComplete());

        $read->closed();

        static::assertTrue($read->isComplete());

        $read->absorb($narrowed);

        static::assertFalse($read->isComplete());
    }

    public function test_count_counts_the_batches_and_closes_the_read_once_exhausted(): void
    {
        $read = new SourceCount();
        $read->opened(false);

        static::assertSame([1, 2, 3], IdRows::ids($read->count(IdRows::batches([1, 2], [3]))));
        static::assertSame(3, $read->rows());
        static::assertTrue($read->isComplete());
    }

    public function test_a_stop_ends_the_count_and_leaves_the_read_open(): void
    {
        $read = new SourceCount();
        $read->opened(false);
        $counted = $read->count(IdRows::batches([1, 2], [3]));

        $counted->current();
        $counted->send(Signal::STOP);

        static::assertFalse($counted->valid());
        static::assertSame(2, $read->rows());
        static::assertFalse($read->isComplete());
    }
}
