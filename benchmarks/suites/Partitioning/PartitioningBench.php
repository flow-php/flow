<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Prices the partition router across key cardinality, and partition pruning against a full read.
 */
final class PartitioningBench
{
    public function warmTree(array $params): void
    {
        PartitionedTree::of(PartitionCardinality::from((string) $params['cardinality']), (int) $params['rows']);
    }

    #[Bench\ParamProviders(['rows', 'low'])]
    #[Bench\Groups(['partitioning'])]
    public function bench_partitioned_write_low(array $params): void
    {
        (new PartitionedWriteScenario(PartitionCardinality::low, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'high'])]
    #[Bench\Groups(['partitioning'])]
    public function bench_partitioned_write_high(array $params): void
    {
        (new PartitionedWriteScenario(PartitionCardinality::high, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'high'])]
    #[Bench\Groups(['partitioning'])]
    #[Bench\BeforeMethods('warmTree')]
    public function bench_partitioned_read_all(array $params): void
    {
        (new PartitionedReadScenario(PartitionCardinality::high, false, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'high'])]
    #[Bench\Groups(['partitioning'])]
    #[Bench\BeforeMethods('warmTree')]
    #[Bench\Revs(10)]
    public function bench_partitioned_read_pruned(array $params): void
    {
        (new PartitionedReadScenario(PartitionCardinality::high, true, (int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function low(): Generator
    {
        yield 'low' => ['cardinality' => PartitionCardinality::low->value];
    }

    public function high(): Generator
    {
        yield 'high' => ['cardinality' => PartitionCardinality::high->value];
    }
}
