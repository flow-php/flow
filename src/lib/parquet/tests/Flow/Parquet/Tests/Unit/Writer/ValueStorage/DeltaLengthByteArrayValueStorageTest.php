<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaLengthByteArrayEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\ValueStorage\DeltaLengthByteArrayValueStorage;
use PHPUnit\Framework\TestCase;

final class DeltaLengthByteArrayValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::DELTA_LENGTH_BYTE_ARRAY, (new DeltaLengthByteArrayValueStorage())->encoding());
    }

    public function test_get_buffer_encodes_non_null_values(): void
    {
        $storage = new DeltaLengthByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['ab', null, '']);

        static::assertSame((new DeltaLengthByteArrayEncoder())->encode(['ab', '']), $storage->getBuffer());
    }

    public function test_get_buffer_without_values_is_an_empty_length_stream(): void
    {
        static::assertSame("\x80\x01\x04\x00\x00", (new DeltaLengthByteArrayValueStorage())->getBuffer());
    }

    public function test_rejects_a_fixed_len_byte_array_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DELTA_LENGTH_BYTE_ARRAY encoding only supports BYTE_ARRAY physical type');

        (new DeltaLengthByteArrayValueStorage())->addValues(FlatColumn::fixedSizeByteArray('c', 4), ['abcd']);
    }

    public function test_rejects_a_non_string_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DELTA_LENGTH_BYTE_ARRAY encoding requires string values, got integer');

        (new DeltaLengthByteArrayValueStorage())->addValues(FlatColumn::string('c'), [1]);
    }

    public function test_reset_clears_values_and_size(): void
    {
        $storage = new DeltaLengthByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['a']);
        $storage->reset();

        static::assertTrue($storage->isEmpty());
        static::assertSame(0, $storage->plainSize());
    }

    public function test_plain_size_is_the_plain_size_of_the_values(): void
    {
        $storage = new DeltaLengthByteArrayValueStorage();
        $storage->addValues(FlatColumn::string('c'), ['ab', null, '']);

        static::assertFalse($storage->isEmpty());
        static::assertSame(10, $storage->plainSize());
    }
}
