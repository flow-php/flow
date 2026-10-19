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
     * or bloom filters prove no row matches $filter. The conjuncts of $filter bound in the file are evaluated on rows
     * (ReadPlan::$rowFilter); the caller evaluates the rest. A limit counts the rows returned, so beside a filter it is
     * accepted only when every conjunct is evaluated on rows.
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

        $rowFilter = $filter->rowFilter();

        if ($limit !== null && $rowFilter !== $filter) {
            throw new InvalidArgumentException(
                'A limit counts the rows returned, which a plan cannot honour unless every conjunct of its filter is '
                . 'bound in the file',
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

        return new ReadPlan($pruned->selected, $pruned->matched, $indexes, $rowFilter, $limit);
    }
}
