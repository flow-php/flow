<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\Reader\Pruning\BloomFilterPruner;
use Flow\Parquet\Reader\Pruning\PageIndexPruner;
use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\Pruning\RowGroupStatisticsPruner;

final readonly class ReadPlanner
{
    /**
     * The rows to read: from $offset on (raw rows), $limit of them, minus the row groups and pages whose statistics
     * or bloom filters prove no row matches $filter. A filtered plan reads a superset of the matching rows, so the caller
     * evaluates the filter; it refuses a limit, which counts the rows the caller returns.
     */
    public function plan(
        Footer $footer,
        Indexes $indexes,
        ?BoundFilter $filter,
        int $offset = 0,
        ?int $limit = null,
    ): ReadPlan {
        $rows = $footer->rowsNumber();

        if ($filter === null || $limit === 0) {
            return ReadPlan::window($rows, $indexes, $offset, $limit);
        }

        if ($limit !== null) {
            throw new InvalidArgumentException(
                'A limit counts the rows returned, which a plan filtered by statistics cannot honour',
            );
        }

        $chunks = $footer->rowGroupChunks($filter->columns());
        $pruned = new PrunedRows(RowSelection::window($rows, $offset), RowSelection::none($rows));

        foreach ([
            new RowGroupStatisticsPruner($chunks),
            new BloomFilterPruner($chunks, $indexes),
            new PageIndexPruner($chunks, $indexes),
        ] as $pruner) {
            $pruned = $pruner->prune($filter, $pruned);
        }

        return new ReadPlan($pruned->selected, $pruned->matched, $indexes);
    }
}
