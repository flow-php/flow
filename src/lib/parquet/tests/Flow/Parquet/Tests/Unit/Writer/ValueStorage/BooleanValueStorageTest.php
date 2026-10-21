<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\ValueStorage\BooleanValueStorage;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_fill;

final class BooleanValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::PLAIN, (new BooleanValueStorage())->encoding());
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
        $storage = new BooleanValueStorage();
        $storage->addValues(FlatColumn::boolean('c'), array_fill(0, $values, true));

        static::assertSame($bytes, $storage->plainSize());
    }
}
