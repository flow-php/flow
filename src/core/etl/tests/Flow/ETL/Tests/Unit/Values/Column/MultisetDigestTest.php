<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\MultisetDigest;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class MultisetDigestTest extends TestCase
{
    public function test_the_empty_digest_is_zero(): void
    {
        static::assertEquals(new MultisetDigest(0, 0), MultisetDigest::empty());
        static::assertSame('00000000000000000000000000000000', MultisetDigest::empty()->hex());
    }

    public function test_a_lane_carries_into_its_upper_half_and_wraps(): void
    {
        static::assertEquals(
            new MultisetDigest(0x1FFFFFFFE, PHP_INT_MIN),
            (new MultisetDigest(0xFFFFFFFF, PHP_INT_MAX))->plus(new MultisetDigest(0xFFFFFFFF, 1)),
        );
    }

    public function test_half_sums_carry_the_low_halves_into_the_high_halves_and_wrap(): void
    {
        static::assertEquals(
            new MultisetDigest(0x0000000300000001, 0x0000000100000000),
            MultisetDigest::ofHalfSums(0x200000001, 1, 0xFFFFFFFF + 0xFFFFFFFF + 2, 0xFFFFFFFF),
        );
    }

    public function test_from_hex_reads_back_both_lanes_at_the_limits(): void
    {
        static::assertEquals(
            new MultisetDigest(PHP_INT_MIN, PHP_INT_MAX),
            MultisetDigest::fromHex((new MultisetDigest(PHP_INT_MIN, PHP_INT_MAX))->hex()),
        );
        static::assertEquals(
            new MultisetDigest(PHP_INT_MAX, PHP_INT_MIN),
            MultisetDigest::fromHex((new MultisetDigest(PHP_INT_MAX, PHP_INT_MIN))->hex()),
        );
    }

    public function test_from_hex_refuses_a_string_that_is_not_32_lowercase_hex_characters(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A multiset digest is 32 lowercase hex characters, high lane first, given: "0000000000000000000000000000000"',
        );

        MultisetDigest::fromHex('0000000000000000000000000000000');
    }

    public function test_from_hex_refuses_uppercase_hex(): void
    {
        $this->expectException(InvalidArgumentException::class);

        MultisetDigest::fromHex('0000000000000000000000000000000A');
    }

    public function test_both_lanes_wrap_past_minus_one(): void
    {
        static::assertEquals(
            new MultisetDigest(-2, 0),
            (new MultisetDigest(-1, PHP_INT_MIN))->plus(new MultisetDigest(-1, PHP_INT_MIN)),
        );
    }

    public function test_hex_is_the_high_lane_then_the_low_lane_in_two_complement(): void
    {
        static::assertSame('800000000000000000000001fffffffe', (new MultisetDigest(0x1FFFFFFFE, PHP_INT_MIN))->hex());
        static::assertSame('0000000000000002ffffffffffffffff', (new MultisetDigest(-1, 2))->hex());
    }
}
