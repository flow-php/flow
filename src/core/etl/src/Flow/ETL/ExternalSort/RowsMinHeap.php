<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Reference, References, SortOrder};

/**
 * @template HeapElement
 *
 * @implements \SplMinHeap<HeapElement>
 */
final class RowsMinHeap extends \SplMinHeap
{
    private readonly References $ref;

    public function __construct(Reference ...$refs)
    {
        $this->ref = References::init(...$refs);
    }

    /**
     * @return CachedRow
     */
    public function extract() : mixed
    {
        return parent::extract();
    }

    #[\ReturnTypeWillChange]
    public function insert(mixed $value) : void
    {
        if (!$value instanceof CachedRow) {
            throw new InvalidArgumentException('Value inserted into RowsMinHeap must be an instance of Flow\\ETL\\ExternalSort\\CachedRow');
        }

        parent::insert($value);
    }

    /**
     * @param CachedRow $value1
     * @param CachedRow $value2
     */
    protected function compare($value1, $value2) : int
    {
        $leftValues = [];
        $rightValues = [];

        foreach ($this->ref as $entry) {
            $row1Value = $value1->row()->valueOf($entry->name());
            $row2Value = $value2->row()->valueOf($entry->name());

            if ($entry->sort() === SortOrder::DESC) {
                $leftValues[] = $row1Value;
                $rightValues[] = $row2Value;
            } else {
                $leftValues[] = $row2Value;
                $rightValues[] = $row1Value;
            }
        }

        return $leftValues <=> $rightValues;
    }
}
