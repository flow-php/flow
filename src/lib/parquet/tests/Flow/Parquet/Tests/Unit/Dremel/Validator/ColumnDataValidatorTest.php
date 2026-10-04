<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\Validator;

use DateTimeImmutable;
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Double\StringableUuid;
use Flow\Parquet\Tests\Double\ToStringUuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class ColumnDataValidatorTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, int}>
     */
    public static function integers_in_range(): Generator
    {
        yield 'INT_8 min' => [new FlatColumn('c', PhysicalType::INT32, ConvertedType::INT_8), -128];
        yield 'INT_16 max' => [new FlatColumn('c', PhysicalType::INT32, ConvertedType::INT_16), 32_767];
        yield 'INT_32 min' => [FlatColumn::int32('c'), -2_147_483_648];
        yield 'plain INT32 max' => [new FlatColumn('c', PhysicalType::INT32), 2_147_483_647];
        yield 'UINT_8 max' => [new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_8), 255];
        yield 'UINT_16 max' => [new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_16), 65_535];
        yield 'UINT_32 max' => [new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_32), 4_294_967_295];
        yield 'UINT_64 max' => [new FlatColumn('c', PhysicalType::INT64, ConvertedType::UINT_64), PHP_INT_MAX];
        yield 'INT_64 min' => [FlatColumn::int64('c'), PHP_INT_MIN];
        yield 'plain INT64 unchecked' => [new FlatColumn('c', PhysicalType::INT64), PHP_INT_MIN];
        yield 'DECIMAL in INT32 unchecked' => [
            new FlatColumn('c', PhysicalType::INT32, logicalType: LogicalType::decimal(2, 9)),
            PHP_INT_MAX,
        ];
    }

    #[DataProvider('integers_in_range')]
    public function test_an_integer_in_the_column_range_is_accepted(FlatColumn $column, int $value): void
    {
        (new ColumnDataValidator())->validate($column, $value, 0);

        $this->expectNotToPerformAssertions();
    }

    /**
     * @return Generator<string, array{FlatColumn, int, string}>
     */
    public static function integers_out_of_range(): Generator
    {
        yield 'INT_8 above' => [
            new FlatColumn('c', PhysicalType::INT32, ConvertedType::INT_8),
            128,
            '128 is out of the INT_8 range [-128, 127]',
        ];
        yield 'INT_16 below' => [
            new FlatColumn('c', PhysicalType::INT32, ConvertedType::INT_16),
            -32_769,
            '-32769 is out of the INT_16 range [-32768, 32767]',
        ];
        yield 'INT_32 above' => [
            FlatColumn::int32('c'),
            4_000_000_000,
            '4000000000 is out of the INT_32 range [-2147483648, 2147483647]',
        ];
        yield 'plain INT32 below' => [
            new FlatColumn('c', PhysicalType::INT32),
            -2_147_483_649,
            '-2147483649 is out of the INT32 range [-2147483648, 2147483647]',
        ];
        yield 'UINT_8 above' => [
            new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_8),
            256,
            '256 is out of the UINT_8 range [0, 255]',
        ];
        yield 'UINT_16 negative' => [
            new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_16),
            -1,
            '-1 is out of the UINT_16 range [0, 65535]',
        ];
        yield 'UINT_32 above' => [
            new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_32),
            4_294_967_296,
            '4294967296 is out of the UINT_32 range [0, 4294967295]',
        ];
        yield 'UINT_64 negative' => [
            new FlatColumn('c', PhysicalType::INT64, ConvertedType::UINT_64),
            -1,
            '-1 is out of the UINT_64 range [0, 9223372036854775807]',
        ];
        yield 'logical INTEGER(8, signed) above' => [
            new FlatColumn('c', PhysicalType::INT32, logicalType: LogicalType::integer(8, true)),
            200,
            '200 is out of the INT_8 range [-128, 127]',
        ];
    }

    #[DataProvider('integers_out_of_range')]
    public function test_an_integer_out_of_the_column_range_is_refused_naming_its_row(
        FlatColumn $column,
        int $value,
        string $message,
    ): void {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Column "c" row 3: ' . $message);

        (new ColumnDataValidator())->validate($column, $value, 3);
    }

    /**
     * @return Generator<string, array{mixed}>
     */
    public static function uuids(): Generator
    {
        yield 'dashed string' => ['0190a6b0-6b1f-7c5e-9a3b-0123456789ab'];
        yield 'dash-less string' => ['0190A6B06B1F7C5E9A3B0123456789AB'];
        yield 'toString() object' => [new ToStringUuid('0190a6b0-6b1f-7c5e-9a3b-0123456789ab')];
        yield 'Stringable object' => [new StringableUuid('0190a6b0-6b1f-7c5e-9a3b-0123456789ab')];
    }

    #[DataProvider('uuids')]
    public function test_a_uuid_of_32_hex_digits_is_accepted(mixed $value): void
    {
        (new ColumnDataValidator())->validate(FlatColumn::uuid('c'), $value, 0);

        $this->expectNotToPerformAssertions();
    }

    /**
     * @return Generator<string, array{mixed, string}>
     */
    public static function not_uuids(): Generator
    {
        yield '16 raw bytes' => ["\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01", 'string'];
        yield 'too short' => ['0190a6b0-6b1f-7c5e-9a3b', 'string'];
        yield 'not hexadecimal' => ['0190a6b0-6b1f-7c5e-9a3b-0123456789az', 'string'];
        yield 'an int' => [1, 'int'];
        yield 'an object without text' => [new DateTimeImmutable('2024-01-01'), 'DateTimeImmutable'];
    }

    #[DataProvider('not_uuids')]
    public function test_a_uuid_that_is_not_32_hex_digits_is_refused(mixed $value, string $type): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Column "c" row 1: ' . $type . ' is not a UUID of 32 hexadecimal digits');

        (new ColumnDataValidator())->validate(FlatColumn::uuid('c'), $value, 1);
    }

    public function test_a_fixed_length_value_of_its_length_is_accepted(): void
    {
        (new ColumnDataValidator())->validate(FlatColumn::fixedSizeByteArray('c', 4), "\x00\x01\x02\x03", 0);

        $this->expectNotToPerformAssertions();
    }

    public function test_a_fixed_length_value_of_another_length_is_refused(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Column "c" row 2: a FIXED_LEN_BYTE_ARRAY(4) value is 5 bytes long');

        (new ColumnDataValidator())->validate(FlatColumn::fixedSizeByteArray('c', 4), '12345', 2);
    }

    /**
     * @return Generator<string, array{FlatColumn}>
     */
    public static function utf8_columns(): Generator
    {
        yield 'STRING' => [FlatColumn::string('c')];
        yield 'JSON' => [FlatColumn::json('c')];
        yield 'ENUM' => [FlatColumn::enum('c')];
    }

    #[DataProvider('utf8_columns')]
    public function test_invalid_utf8_is_refused(FlatColumn $column): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Column "c" row 0: the string is not valid UTF-8');

        (new ColumnDataValidator())->validate($column, "\xFF\xFE", 0);
    }

    #[DataProvider('utf8_columns')]
    public function test_valid_utf8_is_accepted(FlatColumn $column): void
    {
        (new ColumnDataValidator())->validate($column, 'zażółć', 0);

        $this->expectNotToPerformAssertions();
    }

    public function test_invalid_utf8_into_a_plain_byte_array_is_accepted(): void
    {
        (new ColumnDataValidator())->validate(new FlatColumn('c', PhysicalType::BYTE_ARRAY), "\xFF\xFE", 0);

        $this->expectNotToPerformAssertions();
    }
}
