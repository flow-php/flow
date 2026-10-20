<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BloomFilter;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const NAN;

final class BloomFilterOptionsTest extends TestCase
{
    public function test_defaults_are_arrow_rs_defaults(): void
    {
        $options = new BloomFilterOptions();

        static::assertSame(0.05, $options->fpp);
        static::assertNull($options->ndv);
        static::assertSame(1_048_576, BloomFilterOptions::DEFAULT_NDV);
    }

    #[TestWith([0.0])]
    #[TestWith([1.0])]
    #[TestWith([-0.5])]
    #[TestWith([NAN])]
    public function test_a_false_positive_probability_outside_zero_and_one_is_refused(float $fpp): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A bloom filter false positive probability must be between 0 and 1');

        new BloomFilterOptions($fpp);
    }

    public function test_an_ndv_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A bloom filter ndv must be at least 1, got 0');

        new BloomFilterOptions(ndv: 0);
    }

    public function test_the_columns_option_is_read_by_flat_path_and_empty_when_unset(): void
    {
        $id = new BloomFilterOptions(0.01);

        static::assertSame([], BloomFilterOptions::forColumns(new Options()));
        static::assertSame(
            ['id' => $id],
            BloomFilterOptions::forColumns((new Options())->set(Option::COLUMNS_BLOOM_FILTERS, ['id' => $id])),
        );
    }

    public function test_a_columns_option_value_that_is_not_bloom_filter_options_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Option COLUMNS_BLOOM_FILTERS takes BloomFilterOptions, got string for column "id"',
        );

        BloomFilterOptions::forColumns((new Options())->set(Option::COLUMNS_BLOOM_FILTERS, ['id' => 'fast']));
    }
}
