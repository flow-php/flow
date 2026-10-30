<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\IO\Extractor;

interface SplittableExtractor extends Extractor
{
    /**
     * The read of this source with its state resolved (schema, options, inference): what a worker runs for one split,
     * and what the in-process scan delegates to.
     */
    public function reader(): Reader;

    /**
     * Where this source may legally be cut. Called at most once per run, before any worker starts. The splits cover
     * every row exactly once, in source order; each is read through the scan of its kind - a FileSplit through
     * Extractor::extract($backend, $criteria->withSplit($split)).
     *
     * @param Criteria $criteria the read being cut, as the plan hands it to every source today: a file source prunes
     *                           its listing by the path filter, so a pruned file is never cut; any other source reads
     *                           only its limit; the criteria's own split is ignored
     * @param int<1, max> $targetSplitBytes advisory: the caller's preferred split size, not a guarantee
     *
     * @return non-empty-list<Split>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array;
}
