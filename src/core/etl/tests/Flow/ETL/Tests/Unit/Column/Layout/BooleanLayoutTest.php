<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\BooleanLayout;
use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_fill;
use function array_map;
use function bin2hex;

final class BooleanLayoutTest extends TestCase
{
    public function test_packs_bits_lsb_first_with_nulls_as_zero(): void
    {
        static::assertSame(['05'], array_map(bin2hex(...), (new BooleanLayout())->encode([true, null, true, false])));
        static::assertSame(['ff01'], array_map(bin2hex(...), (new BooleanLayout())->encode(array_fill(0, 9, true))));
        static::assertSame(['ff'], array_map(bin2hex(...), (new BooleanLayout())->encode(array_fill(0, 8, true))));
    }

    public function test_round_trips(): void
    {
        $layout = new BooleanLayout();
        $values = [true, null, false, true, true, true, true, true, false];

        static::assertSame($values, $layout->decode(new Buffers($layout->encode($values)), 9, [
            true,
            false,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
        ]));
    }

    public function test_refuses_a_short_buffer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Boolean values buffer of 1 bytes, expected 2 for 9 rows');

        (new BooleanLayout())->decode(new Buffers(["\xFF"]), 9, array_fill(0, 9, true));
    }
}
