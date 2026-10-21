<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\ValueStorage\DeltaBinaryPackedValueStorage;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::DELTA_BINARY_PACKED, (new DeltaBinaryPackedValueStorage())->encoding());
    }

    public function test_get_buffer_encodes_non_null_values(): void
    {
        $storage = new DeltaBinaryPackedValueStorage();
        $storage->addValues(FlatColumn::int32('c'), [1, null, 2]);

        static::assertSame((new DeltaBinaryPackedEncoder())->encode([1, 2]), $storage->getBuffer());
    }

    public function test_get_buffer_without_values_is_an_empty_stream_header(): void
    {
        static::assertSame("\x80\x01\x04\x00\x00", (new DeltaBinaryPackedValueStorage())->getBuffer());
    }

    public function test_rejects_a_non_integer_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Delta encoding requires integer values, got string');

        (new DeltaBinaryPackedValueStorage())->addValues(FlatColumn::int32('c'), ['1']);
    }

    public function test_rejects_a_non_integer_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Delta encoding only supports INT32 and INT64 physical types');

        (new DeltaBinaryPackedValueStorage())->addValues(FlatColumn::string('c'), ['a']);
    }

    public function test_reset_clears_values_and_size(): void
    {
        $storage = new DeltaBinaryPackedValueStorage();
        $storage->addValues(FlatColumn::int32('c'), [1]);
        $storage->reset();

        static::assertTrue($storage->isEmpty());
        static::assertSame(0, $storage->plainSize());
    }

    public function test_plain_size_is_the_plain_size_of_int32_values(): void
    {
        $storage = new DeltaBinaryPackedValueStorage();
        $storage->addValues(FlatColumn::int32('c'), [1, null, 2]);

        static::assertSame(8, $storage->plainSize());
    }

    public function test_plain_size_is_the_plain_size_of_int64_values(): void
    {
        $storage = new DeltaBinaryPackedValueStorage();
        $storage->addValues(FlatColumn::int64('c'), [1, null, 2]);

        static::assertSame(16, $storage->plainSize());
    }
}
