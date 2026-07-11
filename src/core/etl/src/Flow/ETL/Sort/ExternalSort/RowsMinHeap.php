<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Sort\ValueComparator;
use SplMinHeap;

/**
 * @extends \SplMinHeap<BucketRow>
 */
final class RowsMinHeap extends SplMinHeap
{
    /**
     * @var array<int, bool>
     */
    private readonly array $descending;

    /**
     * @var array<int, string>
     */
    private readonly array $names;

    public function __construct(Reference ...$refs)
    {
        $names = [];
        $descending = [];

        foreach (References::init(...$refs) as $ref) {
            $names[] = $ref->name();
            $descending[] = $ref->sort() === SortOrder::DESC;
        }

        $this->names = $names;
        $this->descending = $descending;
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

    /**
     * @return BucketRow
     */
    public function extract(): mixed
    {
        return parent::extract();
    }

    public function insert(mixed $value): true
    {
        if (!$value instanceof BucketRow) {
            throw new InvalidArgumentException(
                'Value inserted into RowsMinHeap must be an instance of Flow\\ETL\\ExternalSort\\CachedRow',
            );
        }

        parent::insert($value);

        return true;
    }

    public function insertRow(Row $row, string $bucketId): void
    {
        $sortValues = [];

        foreach ($this->names as $name) {
            $sortValues[] = $row->valueOf($name);
        }

        $this->insert(new BucketRow($row, $bucketId, $sortValues));
    }

    /**
     * @param BucketRow $value1
     * @param BucketRow $value2
     */
    protected function compare($value1, $value2): int
    {
        foreach ($this->descending as $index => $descending) {
            $comparison = ValueComparator::compare($value2->sortValues[$index], $value1->sortValues[$index]);

            if ($descending) {
                $comparison = -$comparison;
            }

            if ($comparison !== 0) {
                return $comparison;
            }
        }

        return 0;
    }
}
