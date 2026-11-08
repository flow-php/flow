<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation;

use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Tests\FlowTestCase;

final class VoidProgressTest extends FlowTestCase
{
    public function test_a_stage_is_the_same_void_progress(): void
    {
        $progress = new VoidProgress();

        static::assertSame($progress, $progress->stage('sort'));
    }

    public function test_reports_change_nothing(): void
    {
        $progress = new VoidProgress();

        $progress->advanced(1000);
        $progress->reported(500, 'sort', 120, 1000, 2000);

        static::assertEquals(new VoidProgress(), $progress);
    }
}
