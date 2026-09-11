<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

use function array_map;

/**
 * @import-type TimingShape from Timing
 * @import-type BuffersShape from Buffers
 *
 * @type PlanNodeShape = array{
 *     node_type: string,
 *     cost: array{startup_cost: float, total_cost: float},
 *     estimated_rows: int,
 *     row_width: int,
 *     children: array<array<string, mixed>>,
 *     relation_name: ?string,
 *     schema: ?string,
 *     alias: ?string,
 *     index_name: ?string,
 *     index_cond: ?string,
 *     filter: ?string,
 *     timing: ?TimingShape,
 *     buffers: ?BuffersShape,
 *     actual_rows: ?int,
 *     actual_loops: ?int,
 *     rows_removed_by_filter: ?int,
 *     rows_removed_by_index_recheck: ?int,
 *     parent_relationship: ?string,
 *     scan_direction: ?string,
 *     join_type: ?string,
 *     hash_cond: ?string,
 *     sort_key: ?string,
 *     sort_method: ?string,
 *     sort_space_used: ?int,
 *     sort_space_type: ?string,
 *     raw_data: array<array-key, mixed>
 * }
 */
final readonly class PlanNode
{
    /**
     * @param array<PlanNode> $children
     * @param array<array-key, mixed> $rawData
     */
    public function __construct(
        private PlanNodeType $nodeType,
        private Cost $cost,
        private int $estimatedRows,
        private int $rowWidth,
        private array $children = [],
        private ?string $relationName = null,
        private ?string $schema = null,
        private ?string $alias = null,
        private ?string $indexName = null,
        private ?string $indexCond = null,
        private ?string $filter = null,
        private ?Timing $timing = null,
        private ?Buffers $buffers = null,
        private ?int $actualRows = null,
        private ?int $actualLoops = null,
        private ?int $rowsRemovedByFilter = null,
        private ?int $rowsRemovedByIndexRecheck = null,
        private ?string $parentRelationship = null,
        private ?string $scanDirection = null,
        private ?string $joinType = null,
        private ?string $hashCond = null,
        private ?string $sortKey = null,
        private ?string $sortMethod = null,
        private ?int $sortSpaceUsed = null,
        private ?string $sortSpaceType = null,
        private array $rawData = [],
    ) {}

    /**
     * @param PlanNodeShape $data
     */
    public static function fromArray(array $data): self
    {
        $children = [];

        /** @var PlanNodeShape $childData */
        foreach ($data['children'] as $childData) {
            $children[] = self::fromArray($childData);
        }

        return new self(
            nodeType: PlanNodeType::fromString($data['node_type']),
            cost: Cost::fromArray($data['cost']),
            estimatedRows: $data['estimated_rows'],
            rowWidth: $data['row_width'],
            children: $children,
            relationName: $data['relation_name'],
            schema: $data['schema'],
            alias: $data['alias'],
            indexName: $data['index_name'],
            indexCond: $data['index_cond'],
            filter: $data['filter'],
            timing: $data['timing'] !== null ? Timing::fromArray($data['timing']) : null,
            buffers: $data['buffers'] !== null ? Buffers::fromArray($data['buffers']) : null,
            actualRows: $data['actual_rows'],
            actualLoops: $data['actual_loops'],
            rowsRemovedByFilter: $data['rows_removed_by_filter'],
            rowsRemovedByIndexRecheck: $data['rows_removed_by_index_recheck'],
            parentRelationship: $data['parent_relationship'],
            scanDirection: $data['scan_direction'],
            joinType: $data['join_type'],
            hashCond: $data['hash_cond'],
            sortKey: $data['sort_key'],
            sortMethod: $data['sort_method'],
            sortSpaceUsed: $data['sort_space_used'],
            sortSpaceType: $data['sort_space_type'],
            rawData: $data['raw_data'],
        );
    }

    public function actualLoops(): ?int
    {
        return $this->actualLoops;
    }

    public function actualRows(): ?int
    {
        return $this->actualRows;
    }

    public function alias(): ?string
    {
        return $this->alias;
    }

    public function buffers(): ?Buffers
    {
        return $this->buffers;
    }

    /**
     * @return array<PlanNode>
     */
    public function children(): array
    {
        return $this->children;
    }

    public function cost(): Cost
    {
        return $this->cost;
    }

    public function estimatedRows(): int
    {
        return $this->estimatedRows;
    }

    public function filter(): ?string
    {
        return $this->filter;
    }

    public function hasChildren(): bool
    {
        return $this->children !== [];
    }

    public function hashCond(): ?string
    {
        return $this->hashCond;
    }

    public function indexCond(): ?string
    {
        return $this->indexCond;
    }

    public function indexName(): ?string
    {
        return $this->indexName;
    }

    public function isBitmapScan(): bool
    {
        return (
            $this->nodeType === PlanNodeType::BITMAP_HEAP_SCAN
            || $this->nodeType === PlanNodeType::BITMAP_INDEX_SCAN
        );
    }

    public function isHashJoin(): bool
    {
        return $this->nodeType === PlanNodeType::HASH_JOIN;
    }

    public function isIndexOnlyScan(): bool
    {
        return $this->nodeType === PlanNodeType::INDEX_ONLY_SCAN;
    }

    public function isIndexScan(): bool
    {
        return $this->nodeType === PlanNodeType::INDEX_SCAN || $this->nodeType === PlanNodeType::INDEX_ONLY_SCAN;
    }

    public function isMergeJoin(): bool
    {
        return $this->nodeType === PlanNodeType::MERGE_JOIN;
    }

    public function isNestedLoop(): bool
    {
        return $this->nodeType === PlanNodeType::NESTED_LOOP;
    }

    public function isSequentialScan(): bool
    {
        return $this->nodeType === PlanNodeType::SEQ_SCAN;
    }

    public function isSort(): bool
    {
        return $this->nodeType->isSort();
    }

    public function joinType(): ?string
    {
        return $this->joinType;
    }

    public function nodeType(): PlanNodeType
    {
        return $this->nodeType;
    }

    /**
     * @return PlanNodeShape
     */
    public function normalize(): array
    {
        return [
            'node_type' => $this->nodeType->value,
            'cost' => $this->cost->normalize(),
            'estimated_rows' => $this->estimatedRows,
            'row_width' => $this->rowWidth,
            'children' => array_map(static fn(self $child): array => $child->normalize(), $this->children),
            'relation_name' => $this->relationName,
            'schema' => $this->schema,
            'alias' => $this->alias,
            'index_name' => $this->indexName,
            'index_cond' => $this->indexCond,
            'filter' => $this->filter,
            'timing' => $this->timing?->normalize(),
            'buffers' => $this->buffers?->normalize(),
            'actual_rows' => $this->actualRows,
            'actual_loops' => $this->actualLoops,
            'rows_removed_by_filter' => $this->rowsRemovedByFilter,
            'rows_removed_by_index_recheck' => $this->rowsRemovedByIndexRecheck,
            'parent_relationship' => $this->parentRelationship,
            'scan_direction' => $this->scanDirection,
            'join_type' => $this->joinType,
            'hash_cond' => $this->hashCond,
            'sort_key' => $this->sortKey,
            'sort_method' => $this->sortMethod,
            'sort_space_used' => $this->sortSpaceUsed,
            'sort_space_type' => $this->sortSpaceType,
            'raw_data' => $this->rawData,
        ];
    }

    public function parentRelationship(): ?string
    {
        return $this->parentRelationship;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function rawData(): array
    {
        return $this->rawData;
    }

    public function relationName(): ?string
    {
        return $this->relationName;
    }

    public function rowEstimateAccuracy(): ?float
    {
        if ($this->actualRows === null || $this->estimatedRows === 0) {
            return null;
        }

        return $this->actualRows / $this->estimatedRows;
    }

    public function rowsRemovedByFilter(): ?int
    {
        return $this->rowsRemovedByFilter;
    }

    public function rowsRemovedByIndexRecheck(): ?int
    {
        return $this->rowsRemovedByIndexRecheck;
    }

    public function rowWidth(): int
    {
        return $this->rowWidth;
    }

    public function scanDirection(): ?string
    {
        return $this->scanDirection;
    }

    public function schema(): ?string
    {
        return $this->schema;
    }

    public function sortKey(): ?string
    {
        return $this->sortKey;
    }

    public function sortMethod(): ?string
    {
        return $this->sortMethod;
    }

    public function sortSpaceType(): ?string
    {
        return $this->sortSpaceType;
    }

    public function sortSpaceUsed(): ?int
    {
        return $this->sortSpaceUsed;
    }

    public function timing(): ?Timing
    {
        return $this->timing;
    }

    public function usesExternalSort(): bool
    {
        if (!$this->isSort()) {
            return false;
        }

        return $this->sortSpaceType === 'Disk';
    }
}
