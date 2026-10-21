<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use Flow\Parquet\Writer\ValueStorage\RLEBooleanValueStorage;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_fill;

final class RLEBooleanValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::RLE, (new RLEBooleanValueStorage())->encoding());
    }

    public function test_get_buffer_is_the_length_prefixed_hybrid_of_the_non_null_values(): void
    {
        $storage = new RLEBooleanValueStorage();
        $storage->addValues(FlatColumn::boolean('c'), [true, null, false, true]);

        static::assertSame(
            (new RLEBitPackedPacker(new RLEBitPackedHybrid()))->packWithLength(1, [1, 0, 1]),
            $storage->getBuffer(),
        );
    }

    public function test_get_buffer_without_values_is_an_empty_run(): void
    {
        static::assertSame("\x00\x00\x00\x00", (new RLEBooleanValueStorage())->getBuffer());
    }

    public function test_reset_clears_the_values(): void
    {
        $storage = new RLEBooleanValueStorage();
        $storage->addValues(FlatColumn::boolean('c'), [true]);
        $storage->reset();

        static::assertTrue($storage->isEmpty());
        static::assertSame(0, $storage->plainSize());
    }

    /**
     * @param non-negative-int $values
     */
    #[TestWith([0, 0])]
    #[TestWith([1, 1])]
    #[TestWith([8, 1])]
    #[TestWith([9, 2])]
    public function test_plain_size_is_the_bit_packed_plain_size(int $values, int $bytes): void
    {
        $storage = new RLEBooleanValueStorage();
        $storage->addValues(FlatColumn::boolean('c'), array_fill(0, $values, true));

        static::assertSame($bytes, $storage->plainSize());
        static::assertSame($values === 0, $storage->isEmpty());
    }
}
