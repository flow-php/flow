<?php

namespace Flow\ETL\Extractor;

/**
 * Limitable extractor is one that can be limited to extract only given number of rows.
 * Whenever limit is set in a pipeline before any transformations, LogicalPlan processor will try
 * to grab that limit and inject it directly to the extractor to avoid unnecessary processing.
 */
interface LimitableExtractor
{
    public function limit(int $limit): void;
}