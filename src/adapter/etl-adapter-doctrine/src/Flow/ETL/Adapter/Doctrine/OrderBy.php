<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class OrderBy
{
    public function __construct(
        public string $column,
        public Order $order = Order::ASC,
    ) {
    }
}
