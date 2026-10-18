<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader\Pruning;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\BloomEvaluator;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;

/**
 * Drops the row groups whose bloom filters prove no row matches. It proves no match, so matched is unchanged; a group
 * that selects no row, or whose selected rows are all matched already, reads no filter.
 */
final readonly class BloomFilterPruner implements Pruner
{
    public function __construct(
        private RowGroupChunks $chunks,
        private Indexes $indexes,
    ) {}

    public function prune(BoundFilter $filter, PrunedRows $rows): PrunedRows
    {
        $rowCounts = $this->chunks->rowCounts();

        $kept = [];
        $matched = $rows->matched->split($rowCounts);

        foreach ($rows->selected->split($rowCounts) as $rowGroup => $groupSelection) {
            $never =
                $groupSelection->selectsAny()
                && !$matched[$rowGroup]->contains($groupSelection)
                && BloomEvaluator::evaluate($filter, $this->indexes, $rowGroup) === Truth::Never;
            $kept[] = new RowSelector($rowCounts[$rowGroup], $never);
        }

        $selected = $rows->selected->intersection(new RowSelection(...$kept));

        return new PrunedRows($selected, $selected->intersection($rows->matched));
    }
}
