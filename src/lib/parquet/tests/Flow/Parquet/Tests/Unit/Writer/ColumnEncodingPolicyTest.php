<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnEncodingPolicy;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ColumnEncodingPolicyTest extends TestCase
{
    /**
     * @return array<string, array{FlatColumn, int, bool, Encodings}>
     */
    public static function default_encodings_provider(): array
    {
        return [
            'v1 boolean' => [FlatColumn::boolean('c'), 1, true, Encodings::PLAIN],
            'v2 boolean' => [FlatColumn::boolean('c'), 2, true, Encodings::RLE],
            'v1 int32' => [FlatColumn::int32('c'), 1, true, Encodings::RLE_DICTIONARY],
            'v2 int64 without dictionary' => [FlatColumn::int64('c'), 2, false, Encodings::DELTA_BINARY_PACKED],
            'v1 int64 without dictionary' => [FlatColumn::int64('c'), 1, false, Encodings::PLAIN],
            'v2 double without dictionary' => [FlatColumn::double('c'), 2, false, Encodings::PLAIN],
            'v2 string' => [FlatColumn::string('c'), 2, true, Encodings::RLE_DICTIONARY],
            'v2 string without dictionary' => [FlatColumn::string('c'), 2, false, Encodings::DELTA_BYTE_ARRAY],
            'v1 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 1, true, Encodings::PLAIN],
            'v2 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 2, true, Encodings::RLE_DICTIONARY],
            'v2 fixedSizeByteArray without dictionary' => [
                FlatColumn::fixedSizeByteArray('c', 4),
                2,
                false,
                Encodings::DELTA_BYTE_ARRAY,
            ],
        ];
    }

    /**
     * @return array<string, array{FlatColumn, int, Encodings, string}>
     */
    public static function refused_encodings_provider(): array
    {
        return [
            'RLE on int32' => [
                FlatColumn::int32('c'),
                1,
                Encodings::RLE,
                "RLE encoding is only supported for BOOLEAN columns. Column 'c' has type: INT32",
            ],
            'RLE_DICTIONARY on boolean' => [
                FlatColumn::boolean('c'),
                2,
                Encodings::RLE_DICTIONARY,
                "RLE_DICTIONARY encoding is not supported for BOOLEAN columns. Column 'c' has type: BOOLEAN",
            ],
            'RLE_DICTIONARY on fixedSizeByteArray under v1' => [
                FlatColumn::fixedSizeByteArray('c', 4),
                1,
                Encodings::RLE_DICTIONARY,
                'RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1. '
                    . "Column 'c' has type: FIXED_LEN_BYTE_ARRAY",
            ],
            'DELTA_BINARY_PACKED on double' => [
                FlatColumn::double('c'),
                1,
                Encodings::DELTA_BINARY_PACKED,
                "DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. Column 'c' has type: DOUBLE",
            ],
            'DELTA_LENGTH_BYTE_ARRAY on fixedSizeByteArray' => [
                FlatColumn::fixedSizeByteArray('c', 4),
                2,
                Encodings::DELTA_LENGTH_BYTE_ARRAY,
                'DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY columns. '
                    . "Column 'c' has type: FIXED_LEN_BYTE_ARRAY",
            ],
            'DELTA_BYTE_ARRAY on int64' => [
                FlatColumn::int64('c'),
                1,
                Encodings::DELTA_BYTE_ARRAY,
                'DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns. '
                    . "Column 'c' has type: INT64",
            ],
            'BYTE_STREAM_SPLIT' => [
                FlatColumn::double('c'),
                1,
                Encodings::BYTE_STREAM_SPLIT,
                "Encoding 'BYTE_STREAM_SPLIT' is not implemented. Supported encodings: PLAIN, RLE, RLE_DICTIONARY, "
                    . 'DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY',
            ],
        ];
    }

    #[DataProvider('default_encodings_provider')]
    public function test_a_column_without_a_requested_encoding_gets_the_default(
        FlatColumn $column,
        int $writerVersion,
        bool $dictionaryEnabled,
        Encodings $encoding,
    ): void {
        static::assertSame(
            $encoding,
            (new ColumnEncodingPolicy($writerVersion, $dictionaryEnabled))->encodingFor($column),
        );
    }

    public function test_a_requested_dictionary_wins_over_a_disabled_dictionary(): void
    {
        static::assertSame(Encodings::RLE_DICTIONARY, (new ColumnEncodingPolicy(1, false))->encodingFor(
            FlatColumn::string('c'),
            Encodings::RLE_DICTIONARY,
        ));
    }

    public function test_a_requested_encoding_the_column_takes_is_kept(): void
    {
        static::assertSame(Encodings::DELTA_LENGTH_BYTE_ARRAY, (new ColumnEncodingPolicy(1))->encodingFor(
            FlatColumn::string('c'),
            Encodings::DELTA_LENGTH_BYTE_ARRAY,
        ));
    }

    #[DataProvider('refused_encodings_provider')]
    public function test_a_requested_encoding_the_column_cannot_take_is_refused(
        FlatColumn $column,
        int $writerVersion,
        Encodings $encoding,
        string $message,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        (new ColumnEncodingPolicy($writerVersion))->encodingFor($column, $encoding);
    }

    public function test_fallback_encodings_follow_arrow_rs(): void
    {
        $v1 = new ColumnEncodingPolicy(1);
        $v2 = new ColumnEncodingPolicy(2);

        foreach (PhysicalType::cases() as $type) {
            static::assertSame(Encodings::PLAIN, $v1->fallbackEncoding($type), $type->name);
        }

        static::assertSame(Encodings::RLE, $v2->fallbackEncoding(PhysicalType::BOOLEAN));
        static::assertSame(Encodings::DELTA_BINARY_PACKED, $v2->fallbackEncoding(PhysicalType::INT32));
        static::assertSame(Encodings::DELTA_BINARY_PACKED, $v2->fallbackEncoding(PhysicalType::INT64));
        static::assertSame(Encodings::DELTA_BYTE_ARRAY, $v2->fallbackEncoding(PhysicalType::BYTE_ARRAY));
        static::assertSame(Encodings::DELTA_BYTE_ARRAY, $v2->fallbackEncoding(PhysicalType::FIXED_LEN_BYTE_ARRAY));
        static::assertSame(Encodings::PLAIN, $v2->fallbackEncoding(PhysicalType::DOUBLE));
        static::assertSame(Encodings::PLAIN, $v2->fallbackEncoding(PhysicalType::INT96));
    }
}
