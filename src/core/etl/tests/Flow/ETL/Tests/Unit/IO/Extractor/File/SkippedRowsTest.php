<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\Tests\FlowTestCase;

final class SkippedRowsTest extends FlowTestCase
{
    public function test_it_carries_the_rows_skipped(): void
    {
        static::assertSame(3, (new SkippedRows(3))->count);
    }

    public function test_a_negative_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Skipped rows cannot be negative, given: -1');

        new SkippedRows(-1);
    }
}
