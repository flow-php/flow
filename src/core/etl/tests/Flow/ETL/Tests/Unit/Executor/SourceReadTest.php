<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor\SourceRead;
use Flow\ETL\Tests\FlowTestCase;

final class SourceReadTest extends FlowTestCase
{
    public function test_a_source_never_opened_counts_nothing_and_is_complete(): void
    {
        $read = new SourceRead();

        static::assertSame(0, $read->rows());
        static::assertTrue($read->isComplete());
    }

    public function test_an_open_read_is_not_complete_until_it_closes(): void
    {
        $read = new SourceRead();
        $read->opened(false);
        $read->counted(3);

        static::assertFalse($read->isComplete());

        $read->closed();

        static::assertSame(3, $read->rows());
        static::assertTrue($read->isComplete());
    }

    public function test_every_read_must_close(): void
    {
        $read = new SourceRead();
        $read->opened(false);
        $read->opened(false);
        $read->closed();

        static::assertFalse($read->isComplete());
    }

    public function test_a_narrowed_read_never_makes_the_source_complete(): void
    {
        $read = new SourceRead();
        $read->opened(true);
        $read->closed();
        $read->opened(false);
        $read->closed();

        static::assertFalse($read->isComplete());
    }

    public function test_counts_add_up_across_reads(): void
    {
        $read = new SourceRead();
        $read->counted(2);
        $read->counted(5);

        static::assertSame(7, $read->rows());
    }
}
