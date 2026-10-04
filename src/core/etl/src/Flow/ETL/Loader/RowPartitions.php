<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;

final readonly class RowPartitions
{
    public function __construct(
        private References $by,
    ) {}

    public function of(Rows $rows, int $index): Partitions
    {
        $partitions = [];

        foreach ($this->by as $reference) {
            // @mago-ignore analysis:mixed-assignment
            $value = $rows->column($reference->base())->value($index);

            $partitions[] = new Partition(
                $reference->name(),
                $value === null
                    ? null
                    : Partition::fromValue($reference->name(), $rows->schema()->get($reference)->type(), $value),
            );
        }

        // declaration order, not name order: the writer chose this nesting
        return new Partitions(...$partitions);
    }
}
