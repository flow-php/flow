<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\FrameEncoder;
use Flow\Floe\SplittingFrameEncoder;
use Flow\Floe\Tests\Mother\RowsMother;

final class SplittingFrameEncoderTest extends FlowTestCase
{
    public function test_a_batch_that_fits_is_one_frame_holding_every_row(): void
    {
        $rows = RowsMother::workedExample();

        static::assertSame(
            [[(new FrameEncoder())->encode($rows), $rows->count()]],
            (new SplittingFrameEncoder())->encode($rows),
        );
    }
}
