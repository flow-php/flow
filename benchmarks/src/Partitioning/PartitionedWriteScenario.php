<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\Benchmarks\Datasets\Paths;

use function uniqid;

final readonly class PartitionedWriteScenario
{
    public function __construct(
        private PartitionCardinality $cardinality,
        private int $rows,
    ) {}

    public function run(): string
    {
        $root = Paths::var() . '/partition_write_' . $this->cardinality->value . '_' . uniqid('', true);

        (new PartitionedOrders($this->cardinality, $this->rows))->writeTo($root);

        return $root;
    }
}
