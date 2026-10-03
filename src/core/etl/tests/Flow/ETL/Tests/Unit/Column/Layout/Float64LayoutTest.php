<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\Float64Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;

final class Float64LayoutTest extends TestCase
{
    public function test_encodes_nulls_as_zero(): void
    {
        static::assertSame(
            ['000000000000f83f0000000000000000'],
            array_map(bin2hex(...), (new Float64Layout())->encode([1.5, null])),
        );
    }

    public function test_round_trips(): void
    {
        $layout = new Float64Layout();

        static::assertSame(
            [-0.25, null],
            $layout->decode(new Buffers($layout->encode([-0.25, null])), 2, [true, false]),
        );
        static::assertSame([], $layout->decode(new Buffers(['']), 0, []));
    }

    public function test_refuses_a_wrong_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Float64 values buffer of 9 bytes, expected 8 for 1 rows');

        (new Float64Layout())->decode(new Buffers([str_repeat("\0", 9)]), 1, [true]);
    }
}
