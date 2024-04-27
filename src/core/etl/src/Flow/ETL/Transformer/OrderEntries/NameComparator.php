<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;

final class NameComparator implements Comparator
{
    public function __construct(private readonly Order $order = Order::ASC)
    {
    }

    public function compare(Entry $left, Entry $right) : int
    {
        if ($this->order === Order::ASC) {
            return $left->name() <=> $right->name();
        }

        return $right->name() <=> $left->name();
    }
}
