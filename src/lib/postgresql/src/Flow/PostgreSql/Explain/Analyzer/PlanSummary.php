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
     * @param array{
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
     * } $data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            totalCost: $data['total_cost'],
            executionTime: $data['execution_time'],
            planningTime: $data['planning_time'],
            nodeCount: $data['node_count'],
            sequentialScanCount: $data['sequential_scan_count'],
            indexScanCount: $data['index_scan_count'],
            hasExternalSort: $data['has_external_sort'],
            hasDiskReads: $data['has_disk_reads'],
            overallCacheHitRatio: $data['overall_cache_hit_ratio'],
            memoryUsed: $data['memory_used'],
            memoryPeak: $data['memory_peak'],
            hashJoinCount: $data['hash_join_count'],
            nestedLoopCount: $data['nested_loop_count'],
            mergeJoinCount: $data['merge_join_count'],
            totalSharedHit: $data['total_shared_hit'],
            totalSharedRead: $data['total_shared_read'],
            hasTempSpill: $data['has_temp_spill'],
            estimatedRows: $data['estimated_rows'],
            actualRows: $data['actual_rows'],
        );
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
