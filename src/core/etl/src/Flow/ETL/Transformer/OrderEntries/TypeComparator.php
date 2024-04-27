<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;

final class TypeComparator implements Comparator
{
    public function __construct(private readonly TypePriorities $priorities = new TypePriorities(), private readonly Order $order = Order::ASC)
    {
    }

    public function compare(Entry $left, Entry $right) : int
    {
        $leftTypePriority = $this->priorities->for($left);
        $rightTypePriority = $this->priorities->for($right);

        if ($leftTypePriority === $rightTypePriority) {
            return 0;
        }

        if ($this->order === Order::ASC) {
            return $leftTypePriority <=> $rightTypePriority;
        }

        return $rightTypePriority <=> $leftTypePriority;
    }
}
