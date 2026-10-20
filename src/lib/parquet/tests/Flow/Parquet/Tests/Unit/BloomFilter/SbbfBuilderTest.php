<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BloomFilter;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\BloomFilter\SbbfBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\TestCase;

use function str_repeat;
use function strlen;

final class SbbfBuilderTest extends TestCase
{
    public function test_inserted_values_are_found_after_folding(): void
    {
        $builder = SbbfBuilder::for(new BloomFilterOptions(0.05, 1_048_576));

        for ($id = 0; $id < 500; $id++) {
            $builder->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
        }

        $sbbf = $builder->build();

        for ($id = 0; $id < 500; $id++) {
            static::assertTrue($sbbf->check((new BloomHash())->of(FlatColumn::int64('id'), $id)));
        }
    }

    public function test_a_filter_too_full_to_fold_keeps_the_optimal_num_of_bytes(): void
    {
        // arrow-rs: -8 * 1000 / ln(1 - 0.01^(1/8)) = 9681 bits, 1210 bytes, rounded up to 2048
        $builder = SbbfBuilder::for(new BloomFilterOptions(0.01, 1000));

        for ($id = 0; $id < 2000; $id++) {
            $builder->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
        }

        static::assertSame(
            strlen(Sbbf::fromBytes(str_repeat("\0", 2048))->serialized()),
            strlen($builder->build()->serialized()),
        );
    }

    public function test_a_folded_filter_equals_one_built_at_its_size(): void
    {
        $folded = SbbfBuilder::for(new BloomFilterOptions(0.01, 1_048_576));
        $fresh = SbbfBuilder::for(new BloomFilterOptions(0.01, 2000));

        for ($id = 0; $id < 2000; $id++) {
            $folded->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
            $fresh->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
        }

        static::assertSame(
            strlen(Sbbf::fromBytes(str_repeat("\0", 4096))->serialized()),
            strlen($folded->build()->serialized()),
        );
        static::assertEquals($fresh->build(), $folded->build());
    }

    public function test_an_ndv_left_out_is_the_arrow_rs_default(): void
    {
        $default = SbbfBuilder::for(new BloomFilterOptions(0.01));
        $explicit = SbbfBuilder::for(new BloomFilterOptions(0.01, BloomFilterOptions::DEFAULT_NDV));

        for ($id = 0; $id < 2000; $id++) {
            $default->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
            $explicit->insert((new BloomHash())->of(FlatColumn::int64('id'), $id));
        }

        static::assertEquals($explicit->build(), $default->build());
    }

    public function test_an_empty_filter_folds_to_one_block(): void
    {
        static::assertEquals(
            Sbbf::fromBytes(str_repeat("\0", Sbbf::BLOCK_BYTES)),
            SbbfBuilder::for(new BloomFilterOptions(0.05, 1_048_576))->build(),
        );
    }

    public function test_a_one_block_filter_is_not_folded(): void
    {
        $builder = SbbfBuilder::for(new BloomFilterOptions(0.05, 1));
        $builder->insert(-1);

        static::assertSame(
            strlen(Sbbf::fromBytes(str_repeat("\0", Sbbf::BLOCK_BYTES))->serialized()),
            strlen($builder->build()->serialized()),
        );
        static::assertTrue($builder->build()->check(-1));
    }
}
