<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\Buffers;
use Flow\PostgreSql\Explain\Plan\Cost;
use Flow\PostgreSql\Explain\Plan\PlanNode;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use Flow\PostgreSql\Explain\Plan\Timing;
use PHPUnit\Framework\TestCase;

final class PlanNodeTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse(): void
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

        static::assertEquals($original, $restored);
    }

    public function test_from_array_and_normalize_with_nested_children(): void
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

        static::assertEquals($original, $restored);
        static::assertCount(1, $restored->children());
        static::assertEquals(PlanNodeType::INDEX_SCAN, $restored->children()[0]->nodeType());
    }

    public function test_from_array_creates_instance_with_all_fields(): void
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

        static::assertEquals(PlanNodeType::SEQ_SCAN, $node->nodeType());
        static::assertSame(0.0, $node->cost()->startupCost());
        static::assertSame(100.0, $node->cost()->totalCost());
        static::assertSame(1000, $node->estimatedRows());
        static::assertSame(64, $node->rowWidth());
        static::assertSame('users', $node->relationName());
        static::assertSame('public', $node->schema());
        static::assertSame('u', $node->alias());
        static::assertSame('(active = true)', $node->filter());
        static::assertSame(950, $node->actualRows());
        static::assertSame(1, $node->actualLoops());
        static::assertSame(50, $node->rowsRemovedByFilter());
        static::assertSame('Forward', $node->scanDirection());
        static::assertSame(['custom_field' => 'value'], $node->rawData());
        static::assertNotNull($node->timing());
        static::assertSame(0.1, $node->timing()->startupTime());
        static::assertNotNull($node->buffers());
        static::assertSame(50, $node->buffers()->sharedHit());
    }

    public function test_normalize_returns_expected_keys(): void
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

        static::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_normalize_with_null_optional_fields(): void
    {
        $node = new PlanNode(
            nodeType: PlanNodeType::SEQ_SCAN,
            cost: new Cost(0.0, 100.0),
            estimatedRows: 1000,
            rowWidth: 64,
        );

        $normalized = $node->normalize();

        static::assertSame('Seq Scan', $normalized['node_type']);
        static::assertNull($normalized['relation_name']);
        static::assertNull($normalized['schema']);
        static::assertNull($normalized['alias']);
        static::assertNull($normalized['timing']);
        static::assertNull($normalized['buffers']);
        static::assertNull($normalized['actual_rows']);
        static::assertSame([], $normalized['children']);
        static::assertSame([], $normalized['raw_data']);
    }
}
