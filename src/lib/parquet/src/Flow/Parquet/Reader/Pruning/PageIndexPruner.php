<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader\Pruning;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\PageZoneMaps;
use Flow\Parquet\Filter\Evaluation\StatisticsEvaluator;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\PageIndex\PageIndex;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;

use function array_keys;
use function count;
use function sort;

/**
 * A row group is cut at the page boundaries of every filtered column; each part is evaluated with, per column, the
 * page holding it. A group where a filtered column has no usable page index is kept whole; a group whose selected rows
 * are all matched already reads no index.
 */
final readonly class PageIndexPruner implements Pruner
{
    public function __construct(
        private RowGroupChunks $chunks,
        private Indexes $indexes,
    ) {}

    public function prune(BoundFilter $filter, PrunedRows $rows): PrunedRows
    {
        $columns = $filter->columns();

        if ($columns === []) {
            return $rows;
        }

        $rowCounts = $this->chunks->rowCounts();

        $kept = [];
        $proven = [];
        $matched = $rows->matched->split($rowCounts);

        foreach ($rows->selected->split($rowCounts) as $rowGroup => $groupSelection) {
            $groupRows = $rowCounts[$rowGroup];

            if (!$groupSelection->selectsAny()) {
                $kept[] = new RowSelector($groupRows, true);
                $proven[] = new RowSelector($groupRows, true);

                continue;
            }

            if ($matched[$rowGroup]->contains($groupSelection)) {
                $kept[] = new RowSelector($groupRows, false);
                $proven[] = new RowSelector($groupRows, true);

                continue;
            }

            $pageIndexes = [];
            $boundaries = [$groupRows => true];

            foreach ($columns as $column) {
                $pageIndex = PageIndex::of(
                    $this->indexes->columnIndex($rowGroup, $column),
                    $this->indexes->offsetIndex($rowGroup, $column),
                    $groupRows,
                );

                if ($pageIndex === null) {
                    $pageIndexes = null;

                    break;
                }

                $pageIndexes[$column->flatPath()] = $pageIndex;

                foreach ($pageIndex->offsetIndex->pages as $page) {
                    $boundaries[$page->firstRowIndex] = true;
                }
            }

            if ($pageIndexes === null) {
                $kept[] = new RowSelector($groupRows, false);
                $proven[] = new RowSelector($groupRows, true);

                continue;
            }

            $starts = array_keys($boundaries);
            sort($starts);
            $parts = [];

            for ($i = 0; ($i + 1) < count($starts); $i++) {
                $parts[] = $starts[$i + 1] - $starts[$i];
            }

            $zones = new PageZoneMaps($this->chunks, $pageIndexes);

            foreach ($groupSelection->split($parts) as $part => $partSelection) {
                $truth = $partSelection->selectsAny()
                    ? StatisticsEvaluator::evaluate($filter, $zones->at($starts[$part]))
                    : Truth::Never;
                $kept[] = new RowSelector($parts[$part], $truth === Truth::Never);
                $proven[] = new RowSelector($parts[$part], $truth !== Truth::Always);
            }
        }

        $selected = $rows->selected->intersection(new RowSelection(...$kept));

        return new PrunedRows($selected, $selected->intersection($rows->matched->union(new RowSelection(...$proven))));
    }
}
