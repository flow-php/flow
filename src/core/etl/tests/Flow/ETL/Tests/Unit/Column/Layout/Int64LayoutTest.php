<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\Int64Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;

final class Int64LayoutTest extends TestCase
{
    public function test_encodes_nulls_as_zero(): void
    {
        static::assertSame(
            ['01000000000000000000000000000000ffffffffffffffff'],
            array_map(bin2hex(...), (new Int64Layout())->encode([1, null, -1])),
        );
    }

    public function test_round_trips_the_extremes(): void
    {
        $layout = new Int64Layout();

        static::assertSame(
            [PHP_INT_MIN, null, PHP_INT_MAX],
            $layout->decode(new Buffers($layout->encode([PHP_INT_MIN, null, PHP_INT_MAX])), 3, [true, false, true]),
        );
    }

    public function test_decodes_zero_rows(): void
    {
        static::assertSame([], (new Int64Layout())->decode(new Buffers(['']), 0, []));
    }

    public function test_refuses_a_wrong_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Int64 values buffer of 7 bytes, expected 8 for 1 rows');

        (new Int64Layout())->decode(new Buffers(["\0\0\0\0\0\0\0"]), 1, [true]);
    }
}
