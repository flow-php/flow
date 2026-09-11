<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use SplMinHeap;

/**
 * @extends \SplMinHeap<ComparableBucketRow>
 */
final class RowsMinHeap extends SplMinHeap
{
    private readonly References $ref;

    /**
     * @var array<int, SortOrder>
     */
    private readonly array $orders;

    public function __construct(Reference ...$refs)
    {
        $this->ref = References::init(...$refs);

        $orders = [];

        foreach ($this->ref as $ref) {
            $orders[] = $ref->sort();
        }

        $this->orders = $orders;
    }

    public function __debugInfo(): array
    {
        $clone = clone $this;
        $elements = [];

        while (!$clone->isEmpty()) {
            $cachedRow = $clone->extract();
            $elements[] = [$cachedRow->bucketId => $cachedRow->row->toArray()];
        }

        return $elements;
    }

    public function push(Row $row, string $bucketId): void
    {
        $values = [];

        foreach ($this->ref as $ref) {
            $values[] = $row->get($ref->name());
        }

        parent::insert(new ComparableBucketRow($values, $row, $bucketId));
    }

    /**
     * @param ComparableBucketRow $value1
     * @param ComparableBucketRow $value2
     */
    protected function compare($value1, $value2): int
    {
        $left = [];
        $right = [];

        foreach ($this->orders as $index => $order) {
            if ($order === SortOrder::DESC) {
                $left[] = $value1->values[$index];
                $right[] = $value2->values[$index];
            } else {
                $left[] = $value2->values[$index];
                $right[] = $value1->values[$index];
            }
        }

        return $left <=> $right;
    }
}
