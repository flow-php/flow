<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\ByteUnit;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class ByteUnitTest extends FlowTestCase
{
    #[TestWith([-1, 0, 0, 'Byte unit start must not be negative, given: -1'])]
    #[TestWith([0, -1, 0, 'Byte unit size must not be negative, given: -1'])]
    #[TestWith([0, 0, -1, 'Byte unit row count must not be negative, given: -1'])]
    public function test_a_negative_value_is_rejected(int $start, int $bytes, int $rows, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        new ByteUnit($start, $bytes, $rows);
    }

    #[TestWith([100, 50, 125])]
    #[TestWith([100, 51, 125])]
    #[TestWith([100, 0, 100])]
    public function test_the_midpoint_is_the_start_plus_half_the_bytes(int $start, int $bytes, int $midpoint): void
    {
        static::assertSame($midpoint, (new ByteUnit($start, $bytes, 1))->midpoint());
    }

    public function test_zero_is_accepted_everywhere(): void
    {
        $unit = new ByteUnit(0, 0, 0);

        static::assertSame([0, 0, 0], [$unit->start, $unit->bytes, $unit->rows]);
    }
}
