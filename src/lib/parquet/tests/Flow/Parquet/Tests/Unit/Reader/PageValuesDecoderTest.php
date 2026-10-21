<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Data\DeltaByteArrayEncoder;
use Flow\Parquet\Data\DeltaLengthByteArrayEncoder;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader\PageValuesDecoder;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use Flow\Parquet\Writer\ValueStorage\BufferValueStorage;
use Flow\Parquet\Writer\ValueStorage\RLEBooleanValueStorage;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class PageValuesDecoderTest extends TestCase
{
    public function test_a_dictionary_page_without_values_reads_nothing(): void
    {
        $reader = new BinaryBufferReader('tail');

        static::assertSame(
            [],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    $reader,
                    FlatColumn::string('s', Repetition::REQUIRED),
                    Encodings::RLE_DICTIONARY,
                    0,
                    new Dictionary(['a']),
                ),
                false,
            ),
        );
        static::assertSame('tail', $reader->readBytes(4));
    }

    public function test_decodes_delta_binary_packed_int32_wrapping_at_32_bits(): void
    {
        $values = [-2147483648, 2147483647, 0, -1, 1];

        static::assertSame($values, iterator_to_array(
            (new PageValuesDecoder())->decode(
                new BinaryBufferReader((new DeltaBinaryPackedEncoder(physicalType: PhysicalType::INT32))->encode(
                    $values,
                )),
                FlatColumn::int32('i', Repetition::REQUIRED),
                Encodings::DELTA_BINARY_PACKED,
                5,
            ),
            false,
        ));
    }

    public function test_decodes_delta_byte_array(): void
    {
        static::assertSame(
            ['axis', 'axle'],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    new BinaryBufferReader((new DeltaByteArrayEncoder())->encode(['axis', 'axle'])),
                    FlatColumn::fixedSizeByteArray('f', 4, Repetition::REQUIRED),
                    Encodings::DELTA_BYTE_ARRAY,
                    2,
                ),
                false,
            ),
        );
    }

    public function test_decodes_delta_length_byte_array(): void
    {
        static::assertSame(
            ['a', ''],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    new BinaryBufferReader((new DeltaLengthByteArrayEncoder())->encode(['a', ''])),
                    FlatColumn::string('s', Repetition::REQUIRED),
                    Encodings::DELTA_LENGTH_BYTE_ARRAY,
                    2,
                ),
                false,
            ),
        );
    }

    public function test_decodes_dictionary_indices_to_values_and_unknown_indices_to_null(): void
    {
        static::assertSame(
            ['b', 'a', null],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    new BinaryBufferReader((new RLEBitPackedPacker(new RLEBitPackedHybrid()))->packWithBitWidth(2, [
                        1,
                        0,
                        2,
                    ])),
                    FlatColumn::string('s', Repetition::REQUIRED),
                    Encodings::RLE_DICTIONARY,
                    3,
                    new Dictionary(['a', 'b']),
                ),
                false,
            ),
        );
    }

    public function test_decodes_plain(): void
    {
        $column = FlatColumn::int32('i', Repetition::REQUIRED);
        $storage = new BufferValueStorage();
        $storage->addValues($column, [7, -3]);

        static::assertSame(
            [7, -3],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    new BinaryBufferReader($storage->getBuffer()),
                    $column,
                    Encodings::PLAIN,
                    2,
                ),
                false,
            ),
        );
    }

    public function test_decodes_rle_booleans(): void
    {
        $column = FlatColumn::boolean('b', Repetition::REQUIRED);
        $storage = new RLEBooleanValueStorage();
        $storage->addValues($column, [true, false, true]);

        static::assertSame(
            [true, false, true],
            iterator_to_array(
                (new PageValuesDecoder())->decode(
                    new BinaryBufferReader($storage->getBuffer()),
                    $column,
                    Encodings::RLE,
                    3,
                ),
                false,
            ),
        );
    }

    public function test_delta_binary_packed_rejects_a_string_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Delta encoding only supports INT32 and INT64 physical types');

        iterator_to_array((new PageValuesDecoder())->decode(
            new BinaryBufferReader((new DeltaBinaryPackedEncoder())->encode([1])),
            FlatColumn::string('s', Repetition::REQUIRED),
            Encodings::DELTA_BINARY_PACKED,
            1,
        ));
    }

    public function test_delta_byte_array_rejects_a_fixed_len_byte_array_value_of_the_wrong_length(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            "DELTA_BYTE_ARRAY value 1 of FIXED_LEN_BYTE_ARRAY column 'f' has 3 bytes, expected 4",
        );

        iterator_to_array((new PageValuesDecoder())->decode(
            new BinaryBufferReader((new DeltaByteArrayEncoder())->encode(['abcd', 'abc'])),
            FlatColumn::fixedSizeByteArray('f', 4, Repetition::REQUIRED),
            Encodings::DELTA_BYTE_ARRAY,
            2,
        ));
    }

    public function test_delta_byte_array_rejects_an_int32_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'DELTA_BYTE_ARRAY encoding only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY physical types',
        );

        iterator_to_array((new PageValuesDecoder())->decode(
            new BinaryBufferReader((new DeltaByteArrayEncoder())->encode(['a'])),
            FlatColumn::int32('i', Repetition::REQUIRED),
            Encodings::DELTA_BYTE_ARRAY,
            1,
        ));
    }

    public function test_delta_length_byte_array_rejects_an_int32_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('DELTA_LENGTH_BYTE_ARRAY encoding only supports BYTE_ARRAY physical type');

        iterator_to_array((new PageValuesDecoder())->decode(
            new BinaryBufferReader((new DeltaLengthByteArrayEncoder())->encode(['a'])),
            FlatColumn::int32('i', Repetition::REQUIRED),
            Encodings::DELTA_LENGTH_BYTE_ARRAY,
            1,
        ));
    }

    public function test_rle_on_a_non_boolean_column_is_not_supported(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Encoding RLE not supported');

        iterator_to_array((new PageValuesDecoder())->decode(
            new BinaryBufferReader("\x00\x00\x00\x00"),
            FlatColumn::int32('i', Repetition::REQUIRED),
            Encodings::RLE,
            0,
        ));
    }
}
