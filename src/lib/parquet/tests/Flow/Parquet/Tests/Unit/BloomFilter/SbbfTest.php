<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BloomFilter;

use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function pack;
use function str_repeat;

final class SbbfTest extends TestCase
{
    public function test_a_parquet_mr_filter_holds_the_values_it_was_written_with(): void
    {
        $sbbf = Sbbf::fromBytes(pack(
            'C*',
            200,
            1,
            80,
            20,
            64,
            68,
            8,
            109,
            6,
            37,
            4,
            67,
            144,
            80,
            96,
            32,
            8,
            132,
            43,
            33,
            0,
            5,
            99,
            65,
            2,
            0,
            224,
            44,
            64,
            78,
            96,
            4,
        ));

        for ($a = 0; $a < 10; $a++) {
            static::assertTrue($sbbf->check((new BloomHash())->of(FlatColumn::string('a'), 'a' . $a)));
        }
    }

    public function test_an_empty_filter_holds_nothing(): void
    {
        static::assertFalse(Sbbf::fromBytes(str_repeat("\0", 64))->check(-1));
    }

    public function test_bytes_round_trip(): void
    {
        $bytes = str_repeat("\x01\x80", 32);

        static::assertSame($bytes, Sbbf::fromBytes($bytes)->bytes());
    }

    public function test_a_hash_with_the_top_bit_set_lands_in_the_block_and_bits_of_the_unsigned_formulas(): void
    {
        // u64 0xFFFFFFFFFFFFFFFF: block ((2^32 - 1) * 4) >> 32 = 3; word i's bit is the top 5 bits of 2^32 - SALT[i]
        static::assertSame(
            [
                (3 * 256) + 23,
                (3 * 256) + 32 + 23,
                (3 * 256) + 64 + 14,
                (3 * 256) + 96 + 11,
                (3 * 256) + 128 + 17,
                (3 * 256) + 160 + 26,
                (3 * 256) + 192 + 12,
                (3 * 256) + 224 + 20,
            ],
            Sbbf::bits(-1, 4),
        );
    }

    /**
     * @param non-negative-int $length
     */
    #[TestWith([0])]
    #[TestWith([31])]
    #[TestWith([33])]
    public function test_a_bitset_of_no_whole_blocks_is_refused(int $length): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A bloom filter bitset holds whole blocks of 32 bytes, got ' . $length . ' bytes',
        );

        Sbbf::fromBytes(str_repeat("\0", $length));
    }
}
