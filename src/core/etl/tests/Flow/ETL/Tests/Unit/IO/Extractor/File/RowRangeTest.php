<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\RowRange;
use Flow\ETL\Tests\FlowTestCase;

final class RowRangeTest extends FlowTestCase
{
    public function test_a_negative_count_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row range count must not be negative, given: -1');

        new RowRange(0, -1);
    }

    public function test_a_negative_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row range offset must not be negative, given: -1');

        new RowRange(-1, 0);
    }

    public function test_an_empty_range_at_the_start_is_valid(): void
    {
        $range = new RowRange(0, 0);

        static::assertSame([0, 0], [$range->offset, $range->count]);
    }
}
