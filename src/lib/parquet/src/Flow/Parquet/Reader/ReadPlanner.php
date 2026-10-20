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
     * The rows to read: the file's rows from $rawOffset on, minus the row groups and pages whose statistics or bloom
     * filters prove no row matches $filter. The conjuncts of $filter the file answers exactly are evaluated on rows
     * (ReadPlan::$rowFilter); the caller evaluates the rest.
     *
     * @param int $rawOffset rows of the file skipped before any is evaluated
     * @param null|int $limit rows returned at most, counted after the row filter: refused beside a filter whose
     *                        conjuncts are not all evaluated on rows, as the rows returned would still be filtered
     */
    public function plan(
        Footer $footer,
        Indexes $indexes,
        ?BoundFilter $filter,
        int $rawOffset = 0,
        ?int $limit = null,
    ): ReadPlan {
        $rows = $footer->rowsNumber();

        if ($filter === null || $limit === 0) {
            return ReadPlan::window($rows, $indexes, $rawOffset, $limit);
        }

        if ($limit !== null && !$filter->answersEveryConjunct()) {
            throw new InvalidArgumentException(
                'A limit counts the rows returned, which a plan cannot honour unless the file answers every conjunct '
                . 'of its filter exactly',
            );
        }

        $chunks = $footer->rowGroupChunks($filter->columns());
        $pruned = new PrunedRows(RowSelection::window($rows, $rawOffset), RowSelection::none($rows));

        foreach ([
            new RowGroupStatisticsPruner($chunks),
            new BloomFilterPruner($chunks, $indexes),
            new PageIndexPruner($chunks, $indexes),
        ] as $pruner) {
            $pruned = $pruner->prune($filter, $pruned);
        }

        return new ReadPlan($pruned->selected, $pruned->matched, $indexes, $filter->rowFilter(), $limit);
    }
}
