<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class PlanNode
{
    /**
     * @param array<PlanNode> $children
     * @param array<string, mixed> $rawData
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
    ) {
    }

    public function actualLoops() : ?int
    {
        return $this->actualLoops;
    }

    public function actualRows() : ?int
    {
        return $this->actualRows;
    }

    public function alias() : ?string
    {
        return $this->alias;
    }

    public function buffers() : ?Buffers
    {
        return $this->buffers;
    }

    /**
     * @return array<PlanNode>
     */
    public function children() : array
    {
        return $this->children;
    }

    public function cost() : Cost
    {
        return $this->cost;
    }

    public function estimatedRows() : int
    {
        return $this->estimatedRows;
    }

    public function filter() : ?string
    {
        return $this->filter;
    }

    public function hasChildren() : bool
    {
        return $this->children !== [];
    }

    public function hashCond() : ?string
    {
        return $this->hashCond;
    }

    public function indexCond() : ?string
    {
        return $this->indexCond;
    }

    public function indexName() : ?string
    {
        return $this->indexName;
    }

    public function isBitmapScan() : bool
    {
        return $this->nodeType === PlanNodeType::BITMAP_HEAP_SCAN
            || $this->nodeType === PlanNodeType::BITMAP_INDEX_SCAN;
    }

    public function isHashJoin() : bool
    {
        return $this->nodeType === PlanNodeType::HASH_JOIN;
    }

    public function isIndexOnlyScan() : bool
    {
        return $this->nodeType === PlanNodeType::INDEX_ONLY_SCAN;
    }

    public function isIndexScan() : bool
    {
        return $this->nodeType === PlanNodeType::INDEX_SCAN
            || $this->nodeType === PlanNodeType::INDEX_ONLY_SCAN;
    }

    public function isMergeJoin() : bool
    {
        return $this->nodeType === PlanNodeType::MERGE_JOIN;
    }

    public function isNestedLoop() : bool
    {
        return $this->nodeType === PlanNodeType::NESTED_LOOP;
    }

    public function isSequentialScan() : bool
    {
        return $this->nodeType === PlanNodeType::SEQ_SCAN;
    }

    public function isSort() : bool
    {
        return $this->nodeType->isSort();
    }

    public function joinType() : ?string
    {
        return $this->joinType;
    }

    public function nodeType() : PlanNodeType
    {
        return $this->nodeType;
    }

    public function parentRelationship() : ?string
    {
        return $this->parentRelationship;
    }

    /**
     * @return array<string, mixed>
     */
    public function rawData() : array
    {
        return $this->rawData;
    }

    public function relationName() : ?string
    {
        return $this->relationName;
    }

    public function rowEstimateAccuracy() : ?float
    {
        if ($this->actualRows === null || $this->estimatedRows === 0) {
            return null;
        }

        return $this->actualRows / $this->estimatedRows;
    }

    public function rowsRemovedByFilter() : ?int
    {
        return $this->rowsRemovedByFilter;
    }

    public function rowsRemovedByIndexRecheck() : ?int
    {
        return $this->rowsRemovedByIndexRecheck;
    }

    public function rowWidth() : int
    {
        return $this->rowWidth;
    }

    public function scanDirection() : ?string
    {
        return $this->scanDirection;
    }

    public function schema() : ?string
    {
        return $this->schema;
    }

    public function sortKey() : ?string
    {
        return $this->sortKey;
    }

    public function sortMethod() : ?string
    {
        return $this->sortMethod;
    }

    public function sortSpaceType() : ?string
    {
        return $this->sortSpaceType;
    }

    public function sortSpaceUsed() : ?int
    {
        return $this->sortSpaceUsed;
    }

    public function timing() : ?Timing
    {
        return $this->timing;
    }

    public function usesExternalSort() : bool
    {
        if (!$this->isSort()) {
            return false;
        }

        return $this->sortSpaceType === 'Disk';
    }
}
