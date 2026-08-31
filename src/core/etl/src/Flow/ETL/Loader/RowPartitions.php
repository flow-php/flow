<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Schema;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;

final readonly class RowPartitions
{
    public function __construct(
        private References $by,
    ) {}

    public function of(Row $row, Schema $schema): Partitions
    {
        $partitions = [];

        foreach ($this->by as $reference) {
            $value = $row->get($reference);

            $partitions[] = new Partition(
                $reference->name(),
                $value === null
                    ? null
                    : Partition::fromValue($reference->name(), $schema->get($reference)->type(), $value),
            );
        }

        // declaration order, not name order: the writer chose this nesting
        return new Partitions(...$partitions);
    }
}
