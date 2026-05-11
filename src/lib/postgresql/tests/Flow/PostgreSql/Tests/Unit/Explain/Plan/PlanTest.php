<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\Cost;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Explain\Plan\PlanNode;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use Flow\PostgreSql\Explain\Plan\Timing;
use PHPUnit\Framework\TestCase;

final class PlanTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse(): void
    {
        $rootNode = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
            relationName: 'users',
            timing: new Timing(0.1, 5.5, 1),
            actualRows: 950,
            actualLoops: 1,
        );

        $original = new Plan(
            rootNode: $rootNode,
            planningTime: 0.5,
            executionTime: 25.0,
            memoryUsed: 1024,
            memoryPeak: 2048,
        );

        $normalized = $original->normalize();
        $restored = Plan::fromArray($normalized);

        static::assertEquals($original, $restored);
    }

    public function test_from_array_and_normalize_with_nested_plan(): void
    {
        $childNode = new PlanNode(
            nodeType: PlanNodeType::INDEX_SCAN,
            cost: new Cost(0.0, 50.0),
            estimatedRows: 100,
            rowWidth: 32,
            indexName: 'users_pkey',
        );

        $rootNode = new PlanNode(
            nodeType: PlanNodeType::NESTED_LOOP,
            cost: new Cost(0.0, 200.0),
            estimatedRows: 1000,
            rowWidth: 64,
            children: [$childNode],
            joinType: 'Inner',
        );

        $original = new Plan(rootNode: $rootNode, planningTime: 1.0, executionTime: 50.0);

        $normalized = $original->normalize();
        $restored = Plan::fromArray($normalized);

        static::assertEquals($original, $restored);
        static::assertCount(1, $restored->rootNode()->children());
    }

    public function test_from_array_creates_instance(): void
    {
        $data = [
            'root_node' => [
                'node_type' => 'Seq Scan',
                'cost' => ['startup_cost' => 0.0, 'total_cost' => 100.0],
                'estimated_rows' => 1000,
                'row_width' => 64,
                'children' => [],
                'relation_name' => 'users',
                'schema' => null,
                'alias' => null,
                'index_name' => null,
                'index_cond' => null,
                'filter' => null,
                'timing' => null,
                'buffers' => null,
                'actual_rows' => null,
                'actual_loops' => null,
                'rows_removed_by_filter' => null,
                'rows_removed_by_index_recheck' => null,
                'parent_relationship' => null,
                'scan_direction' => null,
                'join_type' => null,
                'hash_cond' => null,
                'sort_key' => null,
                'sort_method' => null,
                'sort_space_used' => null,
                'sort_space_type' => null,
                'raw_data' => [],
            ],
            'planning_time' => 0.5,
            'execution_time' => 25.0,
            'memory_used' => 1024,
            'memory_peak' => 2048,
        ];

        $plan = Plan::fromArray($data);

        static::assertEquals(PlanNodeType::SEQ_SCAN, $plan->rootNode()->nodeType());
        static::assertSame(0.5, $plan->planningTime());
        static::assertSame(25.0, $plan->executionTime());
        static::assertSame(1024, $plan->memoryUsed());
        static::assertSame(2048, $plan->memoryPeak());
    }

    public function test_normalize_returns_all_fields(): void
    {
        $rootNode = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $plan = new Plan(
            rootNode: $rootNode,
            planningTime: 0.5,
            executionTime: 25.0,
            memoryUsed: 1024,
            memoryPeak: 2048,
        );

        $normalized = $plan->normalize();

        static::assertIsArray($normalized['root_node']);
        static::assertSame(0.5, $normalized['planning_time']);
        static::assertSame(25.0, $normalized['execution_time']);
        static::assertSame(1024, $normalized['memory_used']);
        static::assertSame(2048, $normalized['memory_peak']);
    }

    public function test_normalize_returns_expected_keys(): void
    {
        $rootNode = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $plan = new Plan(rootNode: $rootNode);

        $normalized = $plan->normalize();

        $expectedKeys = [
            'root_node',
            'planning_time',
            'execution_time',
            'memory_used',
            'memory_peak',
        ];

        static::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_normalize_with_null_values(): void
    {
        $rootNode = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $plan = new Plan(rootNode: $rootNode);

        $normalized = $plan->normalize();

        static::assertIsArray($normalized['root_node']);
        static::assertNull($normalized['planning_time']);
        static::assertNull($normalized['execution_time']);
        static::assertNull($normalized['memory_used']);
        static::assertNull($normalized['memory_peak']);
    }
}
