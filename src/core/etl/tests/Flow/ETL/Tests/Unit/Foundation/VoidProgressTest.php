<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Tests\FlowTestCase;

final class VoidProgressTest extends FlowTestCase
{
    public function test_a_stage_is_the_same_void_progress(): void
    {
        $progress = new VoidProgress();

        static::assertSame($progress, $progress->stage(StageName::sort));
    }

    public function test_reports_change_nothing(): void
    {
        $progress = new VoidProgress();

        $progress->advanced(1000);
        $progress->reported(500, [], 1000, 2000, false);
        $progress->ended(500, [], 1000, 2000);

        static::assertEquals(new VoidProgress(), $progress);
    }

    public function test_busy_and_done_do_nothing(): void
    {
        $progress = new VoidProgress();

        $progress->busy();
        $progress->done();

        static::assertEquals(new VoidProgress(), $progress);
    }

    public function test_describes_as_itself(): void
    {
        static::assertEquals(Description::compose(VoidProgress::class, []), (new VoidProgress())->describe());
    }

    public function test_counts_in_does_nothing(): void
    {
        $progress = new VoidProgress();

        $progress->countsIn(ProgressUnit::groups);

        static::assertEquals(new VoidProgress(), $progress);
    }

    public function test_nobody_listens_to_it(): void
    {
        static::assertNull((new VoidProgress())->reportsEverySeconds());
    }
}
