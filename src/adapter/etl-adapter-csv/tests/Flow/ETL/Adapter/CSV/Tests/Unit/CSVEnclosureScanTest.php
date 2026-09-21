<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVEnclosureScan;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVRecordBoundaryContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

final class CSVEnclosureScanTest extends TestCase
{
    #[DataProviderExternal(CSVRecordBoundaryContext::class, 'buffers')]
    public function test_the_scan_decides_exactly_like_the_pattern(
        string $separator,
        string $enclosure,
        string $escape,
        string $buffer,
        bool $complete,
    ): void {
        static::assertSame(
            $complete,
            (new CSVEnclosureScan($separator, $enclosure, $escape))->endsOutsideAnEnclosure($buffer),
        );
    }
}
