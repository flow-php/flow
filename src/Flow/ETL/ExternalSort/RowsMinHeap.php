<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Sort;

/**
 * @template HeapElement
 * @implements \SplMinHeap<HeapElement>
 */
final class RowsMinHeap extends \SplMinHeap
{
    /**
     * @var Sort[]
     */
    private array $entries;

    public function __construct(Sort ...$entries)
    {
        $this->entries = $entries;
    }

    /**
     * @param CachedRow $value
     */
    public function insert($value) : void
    {
        if (!$value instanceof CachedRow) {
            throw new InvalidArgumentException('Value inserted into RowsMinHeap must be an instance of Flow\\ETL\\ExternalSort\\CachedRow');
        }

        parent::insert($value);
    }

    /**
     * @return CachedRow
     */
    public function extract()
    {
        return parent::extract();
    }

    /**
     * @param CachedRow $value1
     * @param CachedRow $value2
     */
    protected function compare($value1, $value2) : int
    {
        $values1 = [];
        $values2 = [];

        foreach ($this->entries as $entry) {
            $row1Value = $value1->row()->valueOf($entry->name());
            $row2Value = $value2->row()->valueOf($entry->name());

            $values1[] = $entry->isAsc() === 'asc' ? $row1Value : -$row1Value;
            $values2[] = $entry->isAsc() === 'asc' ? $row2Value : -$row2Value;
        }

        return $values1 <=> $values2;
    }
}
