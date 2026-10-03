<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\Utf8Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;

final class Utf8LayoutTest extends TestCase
{
    public function test_a_null_slot_is_an_empty_range(): void
    {
        static::assertSame(
            ['00000000020000000200000003000000', '616263'],
            array_map(bin2hex(...), (new Utf8Layout())->encode(['ab', null, 'c'])),
        );
    }

    public function test_round_trips(): void
    {
        $layout = new Utf8Layout();

        static::assertSame(
            ['ab', null, '', 'c'],
            $layout->decode(new Buffers($layout->encode(['ab', null, '', 'c'])), 4, [true, false, true, true]),
        );
    }

    public function test_refuses_a_last_offset_other_than_the_data_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Utf8 data buffer of 3 bytes, the last offset is 2');

        (new Utf8Layout())->decode(new Buffers(["\0\0\0\0\x02\0\0\0", 'abc']), 1, [true]);
    }
}
