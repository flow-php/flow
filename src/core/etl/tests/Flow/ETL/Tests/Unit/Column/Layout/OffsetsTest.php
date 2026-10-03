<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Offsets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\OffsetOverflow;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function hex2bin;

final class OffsetsTest extends TestCase
{
    public function test_packs_little_endian_i32(): void
    {
        static::assertSame('000000000200000002000000', bin2hex((new Offsets())->pack([0, 2, 2])));
    }

    public function test_packs_the_last_i32_offset(): void
    {
        static::assertSame('00000000ffffff7f', bin2hex((new Offsets())->pack([0, 2_147_483_647])));
    }

    public function test_refuses_an_offset_past_the_i32_range(): void
    {
        $this->expectException(OffsetOverflow::class);
        $this->expectExceptionMessage('Offsets exceed the i32 range of a batch buffer: the last offset is 2147483648');

        (new Offsets())->pack([0, 2_147_483_648]);
    }

    public function test_unpacks(): void
    {
        static::assertSame([0, 2, 2], (new Offsets())->unpack((string) hex2bin('000000000200000002000000'), 2, 'List'));
    }

    public function test_unpack_refuses_a_wrong_length(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('List offsets buffer of 8 bytes, expected 12 for 2 rows');

        (new Offsets())->unpack((string) hex2bin('0000000002000000'), 2, 'List');
    }

    public function test_unpack_refuses_offsets_that_do_not_start_at_0(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('List offsets start at 1, not 0');

        (new Offsets())->unpack((string) hex2bin('0100000002000000'), 1, 'List');
    }

    public function test_unpack_refuses_offsets_that_decrease(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('List offsets are not monotonic: offset 1 is 3, offset 2 is 2');

        (new Offsets())->unpack((string) hex2bin('000000000300000002000000'), 2, 'List');
    }
}
