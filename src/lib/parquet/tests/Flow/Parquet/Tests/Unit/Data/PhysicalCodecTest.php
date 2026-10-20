<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function str_repeat;
use function var_export;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class PhysicalCodecTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, bool|float|int|string, string}>
     */
    public static function values(): Generator
    {
        yield 'boolean true' => [FlatColumn::boolean('b'), true, "\x01"];
        yield 'boolean false' => [FlatColumn::boolean('b'), false, "\x00"];
        yield 'int32 negative' => [FlatColumn::int32('i'), -2, "\xfe\xff\xff\xff"];
        yield 'int32 date' => [FlatColumn::date('d'), 19000, "\x38\x4a\x00\x00"];
        yield 'int32 decimal unscaled' => [FlatColumn::decimal('d', 9, 2), -350, "\xa2\xfe\xff\xff"];
        yield 'int64 min' => [FlatColumn::int64('i'), PHP_INT_MIN, "\x00\x00\x00\x00\x00\x00\x00\x80"];
        yield 'int64 max' => [FlatColumn::int64('i'), PHP_INT_MAX, "\xff\xff\xff\xff\xff\xff\xff\x7f"];
        yield 'int64 decimal unscaled' => [FlatColumn::decimal('d', 10, 2), -350, "\xa2\xfe\xff\xff\xff\xff\xff\xff"];
        yield 'uint64 bit pattern' => [FlatColumnMother::uint64(), -1, "\xff\xff\xff\xff\xff\xff\xff\xff"];
        yield 'float' => [FlatColumn::float('f'), 1.5, "\x00\x00\xc0\x3f"];
        yield 'double negative zero' => [FlatColumn::double('d'), -0.0, "\x00\x00\x00\x00\x00\x00\x00\x80"];
        yield 'string without length prefix' => [FlatColumn::string('s'), 'abc', 'abc'];
        yield 'binary' => [new FlatColumn('b', PhysicalType::BYTE_ARRAY), "\x00\xff", "\x00\xff"];
        yield 'byte array decimal unscaled bytes' => [FlatColumnMother::byteArrayDecimal(), "\xfe\xa2", "\xfe\xa2"];
        yield 'flba decimal unscaled bytes' => [FlatColumnMother::flbaDecimal(), "\xfe\xa2", "\xfe\xa2"];
        yield 'uuid bytes' => [FlatColumn::uuid('u'), str_repeat("\x01", 16), str_repeat("\x01", 16)];
        yield 'int96 bytes' => [FlatColumnMother::int96(), str_repeat("\x02", 12), str_repeat("\x02", 12)];
    }

    #[DataProvider('values')]
    public function test_round_trip(FlatColumn $column, bool|float|int|string $physical, string $bytes): void
    {
        $codec = new PhysicalCodec();

        static::assertSame($bytes, $codec->encodeStatistic($column, $physical));
        static::assertSame(var_export($physical, true), var_export($codec->decodeStatistic($column, $bytes), true));
    }

    public function test_a_statistic_of_the_wrong_width_decodes_to_null(): void
    {
        static::assertNull((new PhysicalCodec())->decodeStatistic(FlatColumn::int64('i'), "\x00\x00\x00\x00"));
        static::assertNull((new PhysicalCodec())->decodeStatistic(FlatColumn::int32('i'), "\x00"));
    }

    public function test_encoding_a_value_of_the_wrong_type_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "i": string is not a INT64 physical value');

        (new PhysicalCodec())->encodeStatistic(FlatColumn::int64('i'), '1');
    }

    public function test_float_accepts_an_int(): void
    {
        static::assertSame("\x00\x00\x00\x40", (new PhysicalCodec())->encodeStatistic(FlatColumn::float('f'), 2));
        static::assertSame("\x00\x00\x00\x00\x00\x00\x00\x40", (new PhysicalCodec())->encodeStatistic(
            FlatColumn::double('d'),
            2,
        ));
    }
}
