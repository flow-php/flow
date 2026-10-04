<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\Int32Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;

final class Int32LayoutTest extends TestCase
{
    public function test_encodes_nulls_as_zero(): void
    {
        static::assertSame(
            ['fdffffff0000000001000000'],
            array_map(bin2hex(...), (new Int32Layout())->encode([-3, null, 1])),
        );
    }

    public function test_round_trips_signed_values(): void
    {
        $layout = new Int32Layout();
        $values = [-2_147_483_648, null, 2_147_483_647, -3, 0];

        static::assertSame($values, $layout->decode(new Buffers($layout->encode($values)), 5, [
            true,
            false,
            true,
            true,
            true,
        ]));
        static::assertSame([], $layout->decode(new Buffers(['']), 0, []));
    }

    public function test_refuses_a_wrong_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Int32 values buffer of 3 bytes, expected 4 for 1 rows');

        (new Int32Layout())->decode(new Buffers(["\0\0\0"]), 1, [true]);
    }
}
