<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class FlatColumnTest extends TestCase
{
    #[TestWith([1, 1])]
    #[TestWith([6, 3])]
    #[TestWith([7, 4])]
    #[TestWith([11, 5])]
    #[TestWith([12, 6])]
    #[TestWith([18, 8])]
    #[TestWith([19, 9])]
    #[TestWith([23, 10])]
    #[TestWith([24, 11])]
    #[TestWith([35, 15])]
    #[TestWith([36, 16])]
    #[TestWith([38, 16])]
    public function test_decimal_byte_length_follows_the_spec(int $precision, int $length): void
    {
        static::assertSame($length, FlatColumn::decimal('d', $precision, 0)->typeLength());
    }

    #[TestWith([0])]
    #[TestWith([39])]
    public function test_decimal_rejects_precision_out_of_range(int $precision): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Precision must be between 1 and 38, {$precision} given.");

        FlatColumn::decimal('x', $precision, 2);
    }

    public function test_is_map_on_a_non_map_column(): void
    {
        static::assertFalse(FlatColumn::int32('int32')->isMap());
    }

    public function test_repetitions(): void
    {
        static::assertSame(
            [Repetition::OPTIONAL],
            Schema::with(FlatColumn::int32('int32'))->get('int32')->repetitions()->toArray(),
        );
    }
}
