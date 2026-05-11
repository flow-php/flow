<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Analyzer;

use Flow\PostgreSql\Explain\Analyzer\PlanSummary;
use PHPUnit\Framework\TestCase;

final class PlanSummaryTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse(): void
    {
        $original = new PlanSummary(
            totalCost: 150.5,
            executionTime: 25.0,
            planningTime: 0.5,
            nodeCount: 5,
            sequentialScanCount: 2,
            indexScanCount: 3,
            hasExternalSort: true,
            hasDiskReads: true,
            overallCacheHitRatio: 0.95,
            memoryUsed: 1024,
            memoryPeak: 2048,
            hashJoinCount: 1,
            nestedLoopCount: 2,
            mergeJoinCount: 0,
            totalSharedHit: 100,
            totalSharedRead: 5,
            hasTempSpill: false,
            estimatedRows: 1000,
            actualRows: 950,
        );

        $normalized = $original->normalize();
        $restored = PlanSummary::fromArray($normalized);

        static::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance(): void
    {
        $data = [
            'total_cost' => 150.5,
            'execution_time' => 25.0,
            'planning_time' => 0.5,
            'node_count' => 5,
            'sequential_scan_count' => 2,
            'index_scan_count' => 3,
            'has_external_sort' => true,
            'has_disk_reads' => true,
            'overall_cache_hit_ratio' => 0.95,
            'memory_used' => 1024,
            'memory_peak' => 2048,
            'hash_join_count' => 1,
            'nested_loop_count' => 2,
            'merge_join_count' => 0,
            'total_shared_hit' => 100,
            'total_shared_read' => 5,
            'has_temp_spill' => false,
            'estimated_rows' => 1000,
            'actual_rows' => 950,
        ];

        $summary = PlanSummary::fromArray($data);

        static::assertSame(150.5, $summary->totalCost);
        static::assertSame(25.0, $summary->executionTime);
        static::assertSame(0.5, $summary->planningTime);
        static::assertSame(5, $summary->nodeCount);
        static::assertSame(2, $summary->sequentialScanCount);
        static::assertSame(3, $summary->indexScanCount);
        static::assertTrue($summary->hasExternalSort);
        static::assertTrue($summary->hasDiskReads);
        static::assertSame(0.95, $summary->overallCacheHitRatio);
        static::assertSame(1024, $summary->memoryUsed);
        static::assertSame(2048, $summary->memoryPeak);
        static::assertSame(1, $summary->hashJoinCount);
        static::assertSame(2, $summary->nestedLoopCount);
        static::assertSame(0, $summary->mergeJoinCount);
        static::assertSame(100, $summary->totalSharedHit);
        static::assertSame(5, $summary->totalSharedRead);
        static::assertFalse($summary->hasTempSpill);
        static::assertSame(1000, $summary->estimatedRows);
        static::assertSame(950, $summary->actualRows);
    }

    public function test_normalize_returns_all_fields(): void
    {
        $summary = new PlanSummary(
            totalCost: 150.5,
            executionTime: 25.0,
            planningTime: 0.5,
            nodeCount: 5,
            sequentialScanCount: 2,
            indexScanCount: 3,
            hasExternalSort: true,
            hasDiskReads: true,
            overallCacheHitRatio: 0.95,
            memoryUsed: 1024,
            memoryPeak: 2048,
            hashJoinCount: 1,
            nestedLoopCount: 2,
            mergeJoinCount: 0,
            totalSharedHit: 100,
            totalSharedRead: 5,
            hasTempSpill: false,
            estimatedRows: 1000,
            actualRows: 950,
        );

        $normalized = $summary->normalize();

        static::assertSame(150.5, $normalized['total_cost']);
        static::assertSame(25.0, $normalized['execution_time']);
        static::assertSame(0.5, $normalized['planning_time']);
        static::assertSame(5, $normalized['node_count']);
        static::assertSame(2, $normalized['sequential_scan_count']);
        static::assertSame(3, $normalized['index_scan_count']);
        static::assertTrue($normalized['has_external_sort']);
        static::assertTrue($normalized['has_disk_reads']);
        static::assertSame(0.95, $normalized['overall_cache_hit_ratio']);
        static::assertSame(1024, $normalized['memory_used']);
        static::assertSame(2048, $normalized['memory_peak']);
        static::assertSame(1, $normalized['hash_join_count']);
        static::assertSame(2, $normalized['nested_loop_count']);
        static::assertSame(0, $normalized['merge_join_count']);
        static::assertSame(100, $normalized['total_shared_hit']);
        static::assertSame(5, $normalized['total_shared_read']);
        static::assertFalse($normalized['has_temp_spill']);
        static::assertSame(1000, $normalized['estimated_rows']);
        static::assertSame(950, $normalized['actual_rows']);
    }

    public function test_normalize_returns_expected_keys(): void
    {
        $summary = new PlanSummary(
            totalCost: 50.0,
            executionTime: 10.0,
            planningTime: 0.1,
            nodeCount: 2,
            sequentialScanCount: 1,
            indexScanCount: 1,
            hasExternalSort: false,
            hasDiskReads: false,
            overallCacheHitRatio: 1.0,
            memoryUsed: 512,
            memoryPeak: 1024,
            hashJoinCount: 0,
            nestedLoopCount: 0,
            mergeJoinCount: 0,
            totalSharedHit: 50,
            totalSharedRead: 0,
            hasTempSpill: false,
            estimatedRows: 100,
            actualRows: 100,
        );

        $normalized = $summary->normalize();

        $expectedKeys = [
            'total_cost',
            'execution_time',
            'planning_time',
            'node_count',
            'sequential_scan_count',
            'index_scan_count',
            'has_external_sort',
            'has_disk_reads',
            'overall_cache_hit_ratio',
            'memory_used',
            'memory_peak',
            'hash_join_count',
            'nested_loop_count',
            'merge_join_count',
            'total_shared_hit',
            'total_shared_read',
            'has_temp_spill',
            'estimated_rows',
            'actual_rows',
        ];

        static::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_normalize_with_null_values(): void
    {
        $summary = new PlanSummary(
            totalCost: 100.0,
            executionTime: null,
            planningTime: null,
            nodeCount: 1,
            sequentialScanCount: 0,
            indexScanCount: 0,
            hasExternalSort: false,
            hasDiskReads: false,
            overallCacheHitRatio: null,
            memoryUsed: null,
            memoryPeak: null,
            hashJoinCount: 0,
            nestedLoopCount: 0,
            mergeJoinCount: 0,
            totalSharedHit: 0,
            totalSharedRead: 0,
            hasTempSpill: false,
            estimatedRows: 50,
            actualRows: null,
        );

        $normalized = $summary->normalize();

        static::assertSame(100.0, $normalized['total_cost']);
        static::assertNull($normalized['execution_time']);
        static::assertNull($normalized['planning_time']);
        static::assertNull($normalized['overall_cache_hit_ratio']);
        static::assertNull($normalized['memory_used']);
        static::assertNull($normalized['memory_peak']);
        static::assertSame(50, $normalized['estimated_rows']);
        static::assertNull($normalized['actual_rows']);
    }
}
