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

use function min;

final readonly class ReadPlan
{
    /**
     * @param RowSelection $selection rows to read
     * @param RowSelection $matched rows proven to match the filter, within $selection: a hint an engine may ignore,
     *                              evaluating $rowFilter on them as well (arrow does)
     * @param Indexes $indexes the file's page indexes, read once for planning and reading
     * @param null|BoundFilter $rowFilter evaluated on every selected row outside $matched; a row is read when true
     * @param null|int $limit the rows returned at most, counted after $rowFilter
     */
    public function __construct(
        public RowSelection $selection,
        public RowSelection $matched,
        public Indexes $indexes,
        public ?BoundFilter $rowFilter = null,
        public ?int $limit = null,
    ) {
        if ($selection->totalRows() !== $matched->totalRows()) {
            throw new InvalidArgumentException('A read plan selection and its matched rows must cover the same rows');
        }

        if (!$selection->contains($matched)) {
            throw new InvalidArgumentException('A read plan can match only rows it selects');
        }

        if ($limit !== null && $limit < 0) {
            throw new InvalidArgumentException('A read plan limit cannot be negative, got ' . $limit);
        }
    }

    /**
     * The rows to read: the file's rows from $rawOffset on, minus the row groups and pages whose statistics or bloom
     * filters prove no row matches $filter. The conjuncts of $filter the file answers exactly are evaluated on rows
     * (ReadPlan::$rowFilter); the caller evaluates the rest.
     *
     * @param int $rawOffset rows of the file skipped before any is evaluated
     * @param null|int $limit rows returned at most, counted after the row filter: refused beside a filter whose
     *                        conjuncts are not all evaluated on rows, as the rows returned would still be filtered
     * @param null|int $rawRows rows of the file from $rawOffset the plan covers at most, counted before any is
     *                          evaluated - a byte window's row groups; null for all of them
     */
    public static function forFilter(
        Footer $footer,
        Indexes $indexes,
        ?BoundFilter $filter,
        int $rawOffset = 0,
        ?int $limit = null,
        ?int $rawRows = null,
    ): self {
        $rows = $footer->rowsNumber();

        if ($filter === null || $limit === 0) {
            return self::window(
                $rows,
                $indexes,
                $rawOffset,
                $rawRows === null ? $limit : min($limit ?? $rawRows, $rawRows),
            );
        }

        if ($limit !== null && !$filter->answersEveryConjunct()) {
            throw new InvalidArgumentException(
                'A limit counts the rows returned, which a plan cannot honour unless the file answers every conjunct '
                . 'of its filter exactly',
            );
        }

        $chunks = $footer->rowGroupChunks($filter->columns());
        $pruned = new PrunedRows(RowSelection::window($rows, $rawOffset, $rawRows), RowSelection::none($rows));

        foreach ([
            new RowGroupStatisticsPruner($chunks),
            new BloomFilterPruner($chunks, $indexes),
            new PageIndexPruner($chunks, $indexes),
        ] as $pruner) {
            $pruned = $pruner->prune($filter, $pruned);
        }

        return new self($pruned->selected, $pruned->matched, $indexes, $filter->rowFilter(), $limit);
    }

    /**
     * Every row of the window, each a match.
     */
    public static function window(int $rows, Indexes $indexes, int $offset = 0, ?int $limit = null): self
    {
        $selection = RowSelection::window($rows, $offset, $limit);

        return new self($selection, $selection, $indexes);
    }
}
