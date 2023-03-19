<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;

/**
 * @template HeapElement
 *
 * @implements \SplMinHeap<HeapElement>
 */
final class RowsMinHeap extends \SplMinHeap
{
    private readonly References $ref;

    public function __construct(EntryReference ...$refs)
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
        $values1 = [];
        $values2 = [];

        foreach ($this->ref as $entry) {
            $row1Value = $value1->row()->valueOf($entry->name());
            $row2Value = $value2->row()->valueOf($entry->name());

            $values1[] = $entry->sort() === SortOrder::ASC ? $row1Value : -$row1Value;
            $values2[] = $entry->sort() === SortOrder::ASC ? $row2Value : -$row2Value;
        }

        return $values1 <=> $values2;
    }
}
