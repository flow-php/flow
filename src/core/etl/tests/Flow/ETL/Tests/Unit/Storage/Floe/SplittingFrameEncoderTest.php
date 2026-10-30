<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Floe;

use Flow\ETL\Storage\Floe\SplittingFrameEncoder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\PhpBackend;

final class SplittingFrameEncoderTest extends FlowTestCase
{
    public function test_a_batch_that_fits_is_one_frame_holding_every_row(): void
    {
        $rows = RowsMother::workedExample();

        static::assertSame(
            [[(new PhpBackend())->encodeFrame($rows, new NoopCodec()), $rows->count()]],
            (new SplittingFrameEncoder(new PhpBackend()))->encode($rows),
        );
    }
}
