<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\ETL\Rows;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final readonly class PartitionedReadScenario
{
    public function __construct(
        private PartitionCardinality $cardinality,
        private bool $pruned,
        private int $rows,
    ) {}

    public function run(): int
    {
        $tree = PartitionedTree::of($this->cardinality, $this->rows);

        $frame = data_frame(BenchmarkConfig::builder())->read(from_csv($tree->glob()));

        if ($this->pruned) {
            $frame = $frame->filterPartitions(ref($this->cardinality->column())->equals(lit($tree->firstValue())));
        }

        $rows = 0;

        $frame->run(static function (Rows $batch) use (&$rows): void {
            $rows += $batch->count();
        });

        return $rows;
    }
}
