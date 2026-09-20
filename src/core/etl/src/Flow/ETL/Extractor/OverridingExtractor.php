<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

/**
 * A wrapper exposes the extractors it reads so Repeatability can answer for it. The planner does not
 * descend through a wrapper: a wrapped source receives no pushed limit and no path filter.
 *
 * Examples: ChainExtractor, BatchExtractor
 */
interface OverridingExtractor
{
    /**
     * @return array<Extractor>
     */
    public function extractors(): array;
}
