<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Writer\StatisticsTruncation;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function hex2bin;
use function str_repeat;
use function strlen;

/**
 * Cases copied by value from arrow-rs 60 column/writer/mod.rs tests.
 */
final class StatisticsTruncationTest extends TestCase
{
    /**
     * @return Generator<string, array{string, string}>
     */
    public static function increment_utf8(): Generator
    {
        yield 'ascii' => ['hello', 'hellp'];
        yield '1-byte ending in max 1-byte' => ["a\u{7f}", 'b'];
        yield 'utf8' => ['❤️🧡💛💚💙💜', '❤️🧡💛💚💙💝'];
        yield '2-byte without overflow' => ['éééé', 'éééê'];
        yield '2-byte that overflows lowest byte' => ["\u{ff}\u{ff}", "\u{ff}\u{100}"];
        yield '2-byte ending in max 2-byte' => ["a\u{7ff}", 'b'];
        yield '3-byte without overflow' => ['ࠀࠀ', 'ࠀࠁ'];
        yield '3-byte ending in max 3-byte' => ["a\u{ffff}", 'b'];
        yield '4-byte without overflow' => ['𐀀𐀀', '𐀀𐀁'];
        yield '4-byte ending in max unicode' => ["a\u{10ffff}", 'b'];
        yield 'skips the surrogate range' => ["a\u{D7FF}", 'b'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function not_incrementable_utf8(): Generator
    {
        yield 'max 1-byte' => ["\u{7f}\u{7f}"];
        yield 'max 2-byte' => ["\u{7ff}\u{7ff}"];
        yield 'max 3-byte' => ["\u{ffff}\u{ffff}"];
        yield 'max 4-byte' => ["\u{10ffff}\u{10ffff}"];
    }

    /**
     * @return Generator<string, array{FlatColumn, int, string, string, string}>
     */
    public static function truncations(): Generator
    {
        $binary = new FlatColumn('b', PhysicalType::BYTE_ARRAY);
        $utf8 = FlatColumn::string('s');

        yield 'default 64 bytes' => [
            $binary,
            64,
            'This string is longer than 64 bytes, so it will almost certainly be truncated.',
            'This string is longer than 64 bytes, so it will almost certainly',
            'This string is longer than 64 bytes, so it will almost certainlz',
        ];
        yield 'utf8 1 byte' => [$utf8, 1, 'Blart Versenwald III', 'B', 'C'];
        yield 'increment' => [$binary, 3, "\x00\x00\x00\x00", "\x00\x00\x00", "\x00\x00\x01"];
        yield 'increment carries' => [$binary, 3, "\x00\xff\xff\x00", "\x00\xff\xff", "\x01\x00\x00"];
        yield 'increment max binary chars' => [
            $binary,
            5,
            "\xff\xfe\xfd\xff\xff\x00",
            "\xff\xfe\xfd\xff\xff",
            "\xff\xfe\xfe\x00\x00",
        ];
        yield 'utf8 sliced at the char boundary' => [$utf8, 13, '❤️🧡💛💚💙💜', '❤️🧡', '❤️🧢'];
        yield 'ascii truncate and increment' => [$utf8, 8, 'yyyyyyyyy', 'yyyyyyyy', 'yyyyyyyz'];
        yield '2-byte truncate and increment' => [$utf8, 7, 'ééééé', 'ééé', 'ééê'];
        yield '2-byte overflowing lowest byte' => [
            $utf8,
            8,
            "\u{ff}\u{ff}\u{ff}\u{ff}\u{ff}",
            "\u{ff}\u{ff}\u{ff}\u{ff}",
            "\u{ff}\u{ff}\u{ff}\u{100}",
        ];
        yield '3-byte truncate and increment' => [$utf8, 8, 'ࠀࠀࠀࠀ', 'ࠀࠀ', 'ࠀࠁ'];
        yield '4-byte truncate and increment' => [$utf8, 9, '𐀀𐀀𐀀𐀀', '𐀀𐀀', '𐀀𐀁'];
        yield 'invalid utf8 falls back to binary' => [
            $utf8,
            8,
            str_repeat("\x80", 32),
            str_repeat("\x80", 8),
            str_repeat("\x80", 7) . "\x81",
        ];
        yield 'fixed len byte array' => [
            FlatColumn::fixedSizeByteArray('f', 16),
            1,
            (string) hex2bin('0001428a51a8d9cb945f658cee52a7ae'),
            "\x00",
            "\x01",
        ];
    }

    /**
     * @return Generator<string, array{FlatColumn, int, string}>
     */
    public static function untouched(): Generator
    {
        yield 'not longer than the length' => [FlatColumn::string('s'), 9, 'yyyyyyyyy'];
        yield 'decimal fixed len byte array' => [FlatColumnMother::flbaDecimal(), 1, "\x01\x02"];
        yield 'float16' => [FlatColumnMother::float16(), 1, "\x00\x3c"];
        yield 'int64' => [FlatColumn::int64('i'), 1, "\x01\x00\x00\x00\x00\x00\x00\x00"];
        yield 'one code point longer than the length' => [FlatColumn::string('s'), 1, "\u{0836}"];
    }

    #[DataProvider('increment_utf8')]
    public function test_increment_utf8(string $prefix, string $expected): void
    {
        static::assertSame($expected, (new StatisticsTruncation(strlen($prefix)))->max(
            FlatColumn::string('s'),
            $prefix . 'x',
        ));
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_a_length_below_one_is_refused(int $length): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('STATISTICS_TRUNCATE_LENGTH must be greater than 0, ' . $length . ' given');

        new StatisticsTruncation($length);
    }

    #[DataProvider('not_incrementable_utf8')]
    public function test_not_incrementable_utf8_max_stays_whole(string $prefix): void
    {
        $value = $prefix . 'x';

        static::assertSame($value, (new StatisticsTruncation(strlen($prefix)))->max(FlatColumn::string('s'), $value));
    }

    public function test_not_incrementable_binary_max_stays_whole(): void
    {
        $value = "\xff\xff\xff\x00";

        static::assertSame($value, (new StatisticsTruncation(3))->max(
            new FlatColumn('b', PhysicalType::BYTE_ARRAY),
            $value,
        ));
    }

    #[DataProvider('truncations')]
    public function test_truncation(FlatColumn $column, int $length, string $value, string $min, string $max): void
    {
        $truncation = new StatisticsTruncation($length);

        static::assertSame($min, $truncation->min($column, $value));
        static::assertSame($max, $truncation->max($column, $value));
    }

    #[DataProvider('untouched')]
    public function test_untouched(FlatColumn $column, int $length, string $value): void
    {
        $truncation = new StatisticsTruncation($length);

        static::assertSame($value, $truncation->min($column, $value));
        static::assertSame($value, $truncation->max($column, $value));
    }
}
