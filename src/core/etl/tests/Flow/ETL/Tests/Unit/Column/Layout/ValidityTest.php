<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_fill;
use function array_slice;
use function bin2hex;

final class ValidityTest extends TestCase
{
    public function test_no_null_is_an_omitted_bitmap(): void
    {
        static::assertSame('', (new Validity())->fromValues([1, 2, 3]));
        static::assertSame('', (new Validity())->fromValues([]));
        static::assertSame('', (new Validity())->fromNulls([], 3));
    }

    #[TestWith([7, '3f'])]
    #[TestWith([8, '7f'])]
    #[TestWith([9, 'ff00'])]
    public function test_the_last_row_null_packs_lsb_first(int $count, string $hex): void
    {
        $values = array_fill(0, 9, 1);
        $values = array_slice($values, 0, $count);
        $values[$count - 1] = null;

        static::assertSame($hex, bin2hex((new Validity())->fromValues($values)));
        static::assertSame($hex, bin2hex((new Validity())->fromNulls([$count - 1 => true], $count)));
    }

    public function test_the_first_row_null(): void
    {
        static::assertSame('fe01', bin2hex((new Validity())->fromValues([null, 1, 1, 1, 1, 1, 1, 1, 1])));
        static::assertSame('fe01', bin2hex((new Validity())->fromNulls([0 => true], 9)));
    }

    public function test_reads_the_valid_flags(): void
    {
        static::assertSame(
            [false, true, true, true, true, true, true, true, true],
            (new Validity())->valid("\xFE\x01", 9),
        );
        static::assertSame([true, true], (new Validity())->valid('', 2));
        static::assertSame([], (new Validity())->valid('', 0));
    }

    public function test_counts_nulls_ignoring_padding_bits(): void
    {
        static::assertSame(1, (new Validity())->nullCount("\xFE\x01", 9));
        static::assertSame(0, (new Validity())->nullCount('', 9));
        static::assertSame(7, (new Validity())->nullCount("\x01", 8));
    }

    public function test_the_nulls_of_the_flags(): void
    {
        static::assertSame([0 => true, 2 => true], (new Validity())->nulls([false, true, false]));
    }

    public function test_refuses_a_short_bitmap(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Validity bitmap of 1 bytes is too short for 9 rows');

        (new Validity())->valid("\xFF", 9);
    }
}
