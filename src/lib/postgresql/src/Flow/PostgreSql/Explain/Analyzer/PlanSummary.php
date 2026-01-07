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
        public ?int $memoryUsed,
        public ?int $memoryPeak,
        public int $hashJoinCount,
        public int $nestedLoopCount,
        public int $mergeJoinCount,
        public int $totalSharedHit,
        public int $totalSharedRead,
        public bool $hasTempSpill,
        public int $estimatedRows,
        public ?int $actualRows,
    ) {
    }

    /**
     * @return array{
     *     total_cost: float,
     *     execution_time: ?float,
     *     planning_time: ?float,
     *     node_count: int,
     *     sequential_scan_count: int,
     *     index_scan_count: int,
     *     has_external_sort: bool,
     *     has_disk_reads: bool,
     *     overall_cache_hit_ratio: ?float,
     *     memory_used: ?int,
     *     memory_peak: ?int,
     *     hash_join_count: int,
     *     nested_loop_count: int,
     *     merge_join_count: int,
     *     total_shared_hit: int,
     *     total_shared_read: int,
     *     has_temp_spill: bool,
     *     estimated_rows: int,
     *     actual_rows: ?int
     * }
     */
    public function normalize() : array
    {
        return [
            'total_cost' => $this->totalCost,
            'execution_time' => $this->executionTime,
            'planning_time' => $this->planningTime,
            'node_count' => $this->nodeCount,
            'sequential_scan_count' => $this->sequentialScanCount,
            'index_scan_count' => $this->indexScanCount,
            'has_external_sort' => $this->hasExternalSort,
            'has_disk_reads' => $this->hasDiskReads,
            'overall_cache_hit_ratio' => $this->overallCacheHitRatio,
            'memory_used' => $this->memoryUsed,
            'memory_peak' => $this->memoryPeak,
            'hash_join_count' => $this->hashJoinCount,
            'nested_loop_count' => $this->nestedLoopCount,
            'merge_join_count' => $this->mergeJoinCount,
            'total_shared_hit' => $this->totalSharedHit,
            'total_shared_read' => $this->totalSharedRead,
            'has_temp_spill' => $this->hasTempSpill,
            'estimated_rows' => $this->estimatedRows,
            'actual_rows' => $this->actualRows,
        ];
    }
}
