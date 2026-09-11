<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Partitioning;

use Flow\Benchmarks\Datasets\Paths;
use Flow\Benchmarks\Partitioning\PartitionCardinality;
use Flow\Benchmarks\Partitioning\PartitionedReadScenario;
use Flow\Benchmarks\Partitioning\PartitionedTree;
use Flow\Benchmarks\Partitioning\PartitionedWriteScenario;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function basename;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function glob;
use function is_dir;

final class PartitionedTreeTest extends TestCase
{
    public const ROWS = 100;

    public static function cardinalities(): Generator
    {
        foreach (PartitionCardinality::cases() as $cardinality) {
            yield $cardinality->value => [$cardinality];
        }
    }

    #[DataProvider('cardinalities')]
    public function test_it_memoises_the_tree_per_cardinality_and_row_count(PartitionCardinality $cardinality): void
    {
        PartitionedTree::clear();

        try {
            static::assertSame(
                PartitionedTree::of($cardinality, self::ROWS)->glob(),
                PartitionedTree::of($cardinality, self::ROWS)->glob(),
            );
        } finally {
            self::removeTrees();
        }
    }

    #[DataProvider('cardinalities')]
    public function test_the_write_scenario_partitions_by_its_cardinality_column(PartitionCardinality $cardinality): void
    {
        try {
            $root = (new PartitionedWriteScenario($cardinality, self::ROWS))->run();
            $partitions = glob($root . '/*') ?: [];

            static::assertNotSame([], $partitions);

            foreach ($partitions as $partition) {
                static::assertStringStartsWith($cardinality->column() . '=', basename($partition));
            }
        } finally {
            self::removeTrees();
        }
    }

    public function test_a_high_cardinality_key_yields_more_partitions_than_a_low_one(): void
    {
        PartitionedTree::clear();

        try {
            static::assertGreaterThan(
                PartitionedTree::of(PartitionCardinality::low, self::ROWS)->partitionCount(),
                PartitionedTree::of(PartitionCardinality::high, self::ROWS)->partitionCount(),
            );
        } finally {
            self::removeTrees();
        }
    }

    public function test_pruning_a_partition_reads_fewer_rows_than_reading_them_all(): void
    {
        PartitionedTree::clear();

        try {
            $all = (new PartitionedReadScenario(PartitionCardinality::high, false, self::ROWS))->run();
            $pruned = (new PartitionedReadScenario(PartitionCardinality::high, true, self::ROWS))->run();

            static::assertSame(self::ROWS, $all);
            static::assertLessThan($all, $pruned);
            static::assertGreaterThan(0, $pruned);
        } finally {
            self::removeTrees();
        }
    }

    /**
     * Only the write scenario's uniqid() roots under benchmarks/var are removed; the cached tree in
     * the fixture directory is a fixture, and deleting it would defeat the cache.
     */
    public static function removeTrees(): void
    {
        PartitionedTree::clear();

        foreach (glob(Paths::var() . '/partition*') ?: [] as $tree) {
            if (is_dir($tree)) {
                native_local_filesystem()->rm(path($tree));
            }
        }
    }
}
