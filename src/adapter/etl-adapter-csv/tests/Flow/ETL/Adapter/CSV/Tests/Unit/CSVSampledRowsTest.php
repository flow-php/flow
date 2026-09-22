<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVSampledRows;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class CSVSampledRowsTest extends TestCase
{
    public function test_it_carries_what_the_sniff_read(): void
    {
        $sampled = new CSVSampledRows(5, 20, true);

        static::assertSame(5, $sampled->rows);
        static::assertSame(20, $sampled->bytes);
        static::assertTrue($sampled->wholeFile);
    }

    #[TestWith([-1, 0])]
    #[TestWith([0, -1])]
    public function test_negative_counts_are_rejected(int $rows, int $bytes): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Sampled rows and bytes must not be negative, given: ' . $rows . ' rows, ' . $bytes . ' bytes',
        );

        new CSVSampledRows($rows, $bytes, false);
    }
}
