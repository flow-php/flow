<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Analyzer;

final readonly class PlanSummary
{
    public function __construct(
        public float $totalCost,
        public ?float $executionTime,
        public ?float $planningTime,
        public int $nodeCount,
        public int $sequentialScanCount,
        public int $indexScanCount,
        public bool $hasExternalSort,
        public bool $hasDiskReads,
        public ?float $overallCacheHitRatio,
    ) {
    }
}
