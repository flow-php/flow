<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaByteArrayEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\ValueStorage\DeltaByteArrayValueStorage;
use PHPUnit\Framework\TestCase;

final class DeltaByteArrayValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::DELTA_BYTE_ARRAY, (new DeltaByteArrayValueStorage())->encoding());
    }

    public function test_get_buffer_encodes_non_null_values(): void
    {
        $storage = new DeltaByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['axis', null, 'axle']);

        static::assertSame((new DeltaByteArrayEncoder())->encode(['axis', 'axle']), $storage->getBuffer());
    }

    public function test_get_buffer_without_values_is_two_empty_length_streams(): void
    {
        static::assertSame("\x80\x01\x04\x00\x00\x80\x01\x04\x00\x00", (new DeltaByteArrayValueStorage())->getBuffer());
    }

    public function test_rejects_a_non_byte_array_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'DELTA_BYTE_ARRAY encoding only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY physical types',
        );

        (new DeltaByteArrayValueStorage())->addValues(FlatColumn::int64('c'), [1]);
    }

    public function test_rejects_a_non_string_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DELTA_BYTE_ARRAY encoding requires string values, got integer');

        (new DeltaByteArrayValueStorage())->addValues(FlatColumn::string('c'), [1]);
    }

    public function test_reset_clears_values_and_size(): void
    {
        $storage = new DeltaByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['a']);
        $storage->reset();

        static::assertTrue($storage->isEmpty());
        static::assertSame(0, $storage->plainSize());
    }

    public function test_plain_size_is_the_plain_size_of_byte_array_values(): void
    {
        $storage = new DeltaByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['ab', null, '']);

        static::assertFalse($storage->isEmpty());
        static::assertSame(10, $storage->plainSize());
    }

    public function test_plain_size_is_the_plain_size_of_fixed_len_byte_array_values(): void
    {
        $storage = new DeltaByteArrayValueStorage();
        $storage->addValues(FlatColumn::fixedSizeByteArray('c', 4), ['abcd', null, 'abce']);

        static::assertSame(8, $storage->plainSize());
    }
}
