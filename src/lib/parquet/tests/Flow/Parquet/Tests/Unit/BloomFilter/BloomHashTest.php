<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BloomFilter;

use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use PHPUnit\Framework\TestCase;

final class BloomHashTest extends TestCase
{
    public function test_byte_arrays_hash_without_their_length_prefix(): void
    {
        // xxh64 seed 0: "" = 0xef46db3751d8e999, "a" = 0xd24ec4f1a98c6e5b, "abc" = 0x44bc2cf5ad770999
        static::assertSame(-1_205_034_819_632_174_695, (new BloomHash())->of(FlatColumn::string('s'), ''));
        static::assertSame(-3_292_477_735_350_538_661, (new BloomHash())->of(FlatColumn::string('s'), 'a'));
        static::assertSame(4_952_883_123_889_572_249, (new BloomHash())->of(FlatColumn::string('s'), 'abc'));
    }

    public function test_integers_hash_as_little_endian_bytes(): void
    {
        // xxh64("\x01\x00\x00\x00") = 0xf42f94001fcb5351, xxh64(8 x "\xff") = 0x85d136adb773c6c9
        static::assertSame(-851_299_076_295_404_719, (new BloomHash())->of(FlatColumn::int32('i'), 1));
        static::assertSame(-8_804_195_676_797_548_855, (new BloomHash())->of(FlatColumn::int64('l'), -1));
    }

    public function test_an_unsigned_64_bit_value_hashes_as_its_int64_bit_pattern(): void
    {
        static::assertSame(
            (new BloomHash())->of(FlatColumn::int64('l'), -1),
            (new BloomHash())->of(
                new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)),
                -1,
            ),
        );
    }

    public function test_floating_point_values_hash_as_little_endian_ieee_754(): void
    {
        // xxh64 of 1.5 as f32 = 0x4f2d82595c483a0d, as f64 = 0x49f7b96b6b5ccaf9
        static::assertSame(5_705_359_623_260_224_013, (new BloomHash())->of(FlatColumn::float('f'), 1.5));
        static::assertSame(5_329_932_555_030_153_977, (new BloomHash())->of(FlatColumn::double('d'), 1.5));
    }

    public function test_a_boolean_hashes_as_one_byte(): void
    {
        // xxh64("\x01") = 0x8a4127811b21e730
        static::assertSame(-8_484_456_787_529_832_656, (new BloomHash())->of(FlatColumn::boolean('b'), true));
    }
}
