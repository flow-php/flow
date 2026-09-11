<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Window;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Partitioning\PartitionCardinality;
use Flow\Benchmarks\Window\WindowPartitioning;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final class WindowPartitioningTest extends TestCase
{
    public const ROWS = 100;

    /**
     * If only the high arm derived the key, the cardinality delta would also carry a per-row temporal
     * transform - which is what the axis exists to exclude.
     */
    public function test_both_arms_derive_the_key_so_only_the_partition_column_varies(): void
    {
        foreach (PartitionCardinality::cases() as $cardinality) {
            static::assertContains(
                'day',
                (new WindowPartitioning($cardinality))
                    ->derive(data_frame()->read(from_floe(Datasets::orders(self::ROWS)->floe())))
                    ->fetch()
                    ->first()
                    ->names(),
                $cardinality->value,
            );
        }
    }

    public function test_the_reference_names_the_cardinality_column(): void
    {
        foreach (PartitionCardinality::cases() as $cardinality) {
            static::assertSame($cardinality->column(), (new WindowPartitioning($cardinality))->reference()->name());
        }
    }
}
