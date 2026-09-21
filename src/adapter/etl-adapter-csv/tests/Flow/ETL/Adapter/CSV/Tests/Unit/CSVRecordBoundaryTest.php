<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVRecordBoundary;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVRecordBoundaryContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function str_repeat;

final class CSVRecordBoundaryTest extends TestCase
{
    #[DataProviderExternal(CSVRecordBoundaryContext::class, 'buffers')]
    public function test_a_record_is_complete_when_the_buffer_ends_outside_an_enclosure(
        string $separator,
        string $enclosure,
        string $escape,
        string $buffer,
        bool $complete,
    ): void {
        static::assertSame($complete, (new CSVRecordBoundary($enclosure, $separator, $escape))->isComplete($buffer));
    }

    public function test_a_record_with_more_fields_than_pcre_can_match_is_decided_by_the_scan(): void
    {
        $boundary = new CSVRecordBoundary('"');

        static::assertTrue($boundary->isComplete(str_repeat('"a",', 200_000) . '"b"'));
        static::assertFalse($boundary->isComplete(str_repeat('"a",', 200_000) . '"b'));
    }
}
