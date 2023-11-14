<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;

/**
 * Extractors implementing OverridingExtractor interface overrides one or more extractors.
 * This interface is required by Execution Plan / Optimizer to fully understand execution plan.
 *
 * Examples: ChainLoader
 */
interface OverridingExtractor
{
    /**
     * @return array<Extractor>
     */
    public function extractors() : array;
}
