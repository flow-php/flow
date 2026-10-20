<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader\Pruning;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\RowGroupZoneMaps;
use Flow\Parquet\Filter\Evaluation\StatisticsEvaluator;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;

final readonly class RowGroupStatisticsPruner implements Pruner
{
    public function __construct(
        private RowGroupChunks $chunks,
    ) {}

    public function prune(BoundFilter $filter, PrunedRows $rows): PrunedRows
    {
        $rowCounts = $this->chunks->rowCounts();

        $kept = [];
        $proven = [];

        foreach ($rows->selected->split($rowCounts) as $rowGroup => $groupSelection) {
            $truth = $groupSelection->selectsAny()
                ? StatisticsEvaluator::evaluate($filter, new RowGroupZoneMaps($this->chunks, $rowGroup))
                : Truth::Never;
            $kept[] = new RowSelector($rowCounts[$rowGroup], $truth === Truth::Never);
            $proven[] = new RowSelector($rowCounts[$rowGroup], $truth !== Truth::Always);
        }

        $selected = $rows->selected->intersection(new RowSelection(...$kept));

        return new PrunedRows($selected, $selected->intersection($rows->matched->union(new RowSelection(...$proven))));
    }
}
