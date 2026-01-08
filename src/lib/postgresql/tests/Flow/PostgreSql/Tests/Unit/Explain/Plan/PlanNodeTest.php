<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\{Buffers, Cost, PlanNode, PlanNodeType, Timing};
use PHPUnit\Framework\TestCase;

final class PlanNodeTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse() : void
    {
        $original = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
            children: [],
            relationName: 'users',
            schema: 'public',
            alias: 'u',
            indexName: null,
            indexCond: null,
            filter: '(active = true)',
            timing: new Timing(0.1, 5.5, 1),
            buffers: new Buffers(50, 10, 0, 0, 0, 0, 0, 0, 0, 0),
            actualRows: 950,
            actualLoops: 1,
            rowsRemovedByFilter: 50,
            rowsRemovedByIndexRecheck: null,
            parentRelationship: null,
            scanDirection: 'Forward',
            joinType: null,
            hashCond: null,
            sortKey: null,
            sortMethod: null,
            sortSpaceUsed: null,
            sortSpaceType: null,
            rawData: ['custom_field' => 'value'],
        );

        $normalized = $original->normalize();
        $restored = PlanNode::fromArray($normalized);

        self::assertEquals($original, $restored);
    }

    public function test_from_array_and_normalize_with_nested_children() : void
    {
        $childNode = new PlanNode(
            nodeType: PlanNodeType::INDEX_SCAN,
            cost: new Cost(0.0, 50.0),
            estimatedRows: 100,
            rowWidth: 32,
        );

        $original = new PlanNode(
            nodeType: PlanNodeType::NESTED_LOOP,
            cost: new Cost(0.0, 200.0),
            estimatedRows: 1000,
            rowWidth: 64,
            children: [$childNode],
            joinType: 'Inner',
        );

        $normalized = $original->normalize();
        $restored = PlanNode::fromArray($normalized);

        self::assertEquals($original, $restored);
        self::assertCount(1, $restored->children());
        self::assertEquals(PlanNodeType::INDEX_SCAN, $restored->children()[0]->nodeType());
    }

    public function test_from_array_creates_instance_with_all_fields() : void
    {
        $data = [
            'node_type' => 'Seq Scan',
            'cost' => ['startup_cost' => 0.0, 'total_cost' => 100.0],
            'estimated_rows' => 1000,
            'row_width' => 64,
            'children' => [],
            'relation_name' => 'users',
            'schema' => 'public',
            'alias' => 'u',
            'index_name' => null,
            'index_cond' => null,
            'filter' => '(active = true)',
            'timing' => ['startup_time' => 0.1, 'total_time' => 5.5, 'loops' => 1],
            'buffers' => [
                'shared_hit' => 50,
                'shared_read' => 10,
                'shared_dirtied' => 0,
                'shared_written' => 0,
                'local_hit' => 0,
                'local_read' => 0,
                'local_dirtied' => 0,
                'local_written' => 0,
                'temp_read' => 0,
                'temp_written' => 0,
            ],
            'actual_rows' => 950,
            'actual_loops' => 1,
            'rows_removed_by_filter' => 50,
            'rows_removed_by_index_recheck' => null,
            'parent_relationship' => null,
            'scan_direction' => 'Forward',
            'join_type' => null,
            'hash_cond' => null,
            'sort_key' => null,
            'sort_method' => null,
            'sort_space_used' => null,
            'sort_space_type' => null,
            'raw_data' => ['custom_field' => 'value'],
        ];

        $node = PlanNode::fromArray($data);

        self::assertEquals(PlanNodeType::SEQ_SCAN, $node->nodeType());
        self::assertSame(0.0, $node->cost()->startupCost());
        self::assertSame(100.0, $node->cost()->totalCost());
        self::assertSame(1000, $node->estimatedRows());
        self::assertSame(64, $node->rowWidth());
        self::assertSame('users', $node->relationName());
        self::assertSame('public', $node->schema());
        self::assertSame('u', $node->alias());
        self::assertSame('(active = true)', $node->filter());
        self::assertSame(950, $node->actualRows());
        self::assertSame(1, $node->actualLoops());
        self::assertSame(50, $node->rowsRemovedByFilter());
        self::assertSame('Forward', $node->scanDirection());
        self::assertSame(['custom_field' => 'value'], $node->rawData());
        self::assertNotNull($node->timing());
        self::assertSame(0.1, $node->timing()->startupTime());
        self::assertNotNull($node->buffers());
        self::assertSame(50, $node->buffers()->sharedHit());
    }

    public function test_normalize_returns_expected_keys() : void
    {
        $node = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $normalized = $node->normalize();

        $expectedKeys = [
            'node_type',
            'cost',
            'estimated_rows',
            'row_width',
            'children',
            'relation_name',
            'schema',
            'alias',
            'index_name',
            'index_cond',
            'filter',
            'timing',
            'buffers',
            'actual_rows',
            'actual_loops',
            'rows_removed_by_filter',
            'rows_removed_by_index_recheck',
            'parent_relationship',
            'scan_direction',
            'join_type',
            'hash_cond',
            'sort_key',
            'sort_method',
            'sort_space_used',
            'sort_space_type',
            'raw_data',
        ];

        self::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_normalize_with_null_optional_fields() : void
    {
        $node = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $normalized = $node->normalize();

        self::assertSame('Seq Scan', $normalized['node_type']);
        self::assertNull($normalized['relation_name']);
        self::assertNull($normalized['schema']);
        self::assertNull($normalized['alias']);
        self::assertNull($normalized['timing']);
        self::assertNull($normalized['buffers']);
        self::assertNull($normalized['actual_rows']);
        self::assertSame([], $normalized['children']);
        self::assertSame([], $normalized['raw_data']);
    }
}
