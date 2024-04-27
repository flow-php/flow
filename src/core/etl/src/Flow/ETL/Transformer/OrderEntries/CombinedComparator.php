<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;

final class CombinedComparator implements Comparator
{
    public function __construct(private readonly Comparator $first, private readonly Comparator $second)
    {
    }

    public function compare(Entry $left, Entry $right) : int
    {
        $result = $this->first->compare($left, $right);

        if ($result === 0) {
            return $this->second->compare($left, $right);
        }

        return $result;
    }
}
