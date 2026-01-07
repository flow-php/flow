<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Analyzer;

use Flow\PostgreSql\Explain\Analyzer\PlanSummary;
use PHPUnit\Framework\TestCase;

final class PlanSummaryTest extends TestCase
{
    public function test_normalize_returns_all_fields() : void
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

        self::assertSame(150.5, $normalized['total_cost']);
        self::assertSame(25.0, $normalized['execution_time']);
        self::assertSame(0.5, $normalized['planning_time']);
        self::assertSame(5, $normalized['node_count']);
        self::assertSame(2, $normalized['sequential_scan_count']);
        self::assertSame(3, $normalized['index_scan_count']);
        self::assertTrue($normalized['has_external_sort']);
        self::assertTrue($normalized['has_disk_reads']);
        self::assertSame(0.95, $normalized['overall_cache_hit_ratio']);
        self::assertSame(1024, $normalized['memory_used']);
        self::assertSame(2048, $normalized['memory_peak']);
        self::assertSame(1, $normalized['hash_join_count']);
        self::assertSame(2, $normalized['nested_loop_count']);
        self::assertSame(0, $normalized['merge_join_count']);
        self::assertSame(100, $normalized['total_shared_hit']);
        self::assertSame(5, $normalized['total_shared_read']);
        self::assertFalse($normalized['has_temp_spill']);
        self::assertSame(1000, $normalized['estimated_rows']);
        self::assertSame(950, $normalized['actual_rows']);
    }

    public function test_normalize_returns_expected_keys() : void
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

        self::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_normalize_with_null_values() : void
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

        self::assertSame(100.0, $normalized['total_cost']);
        self::assertNull($normalized['execution_time']);
        self::assertNull($normalized['planning_time']);
        self::assertNull($normalized['overall_cache_hit_ratio']);
        self::assertNull($normalized['memory_used']);
        self::assertNull($normalized['memory_peak']);
        self::assertSame(50, $normalized['estimated_rows']);
        self::assertNull($normalized['actual_rows']);
    }
}
