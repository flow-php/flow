<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonSampledRows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class JsonSampledRowsTest extends FlowTestCase
{
    #[TestWith([-1, 0])]
    #[TestWith([0, -1])]
    public function test_negative_rows_or_bytes_are_refused(int $rows, int $bytes): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Sampled rows and bytes must not be negative, given: ' . $rows . ' rows, ' . $bytes . ' bytes',
        );

        new JsonSampledRows($rows, $bytes, false);
    }

    public function test_it_keeps_what_it_was_given(): void
    {
        $sampled = new JsonSampledRows(3, 27, true);

        static::assertSame(3, $sampled->rows);
        static::assertSame(27, $sampled->bytes);
        static::assertTrue($sampled->wholeFile);
    }
}
