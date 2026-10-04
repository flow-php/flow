<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\FixedBinary16Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function str_repeat;

final class FixedBinary16LayoutTest extends TestCase
{
    public function test_a_null_slot_is_sixteen_zero_bytes(): void
    {
        static::assertSame(
            [str_repeat('a', 16) . str_repeat("\0", 16)],
            (new FixedBinary16Layout())->encode([str_repeat('a', 16), null]),
        );
    }

    public function test_round_trips(): void
    {
        $layout = new FixedBinary16Layout();
        $values = [str_repeat('a', 16), null, str_repeat('b', 16)];

        static::assertSame($values, $layout->decode(new Buffers($layout->encode($values)), 3, [true, false, true]));
        static::assertSame([], $layout->decode(new Buffers(['']), 0, []));
    }

    public function test_refuses_a_wrong_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('FixedBinary16 values buffer of 15 bytes, expected 16 for 1 rows');

        (new FixedBinary16Layout())->decode(new Buffers([str_repeat('a', 15)]), 1, [true]);
    }
}
