<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final class OrderBy
{
    public function __construct(
        public readonly string $column,
        public readonly Order $order = Order::ASC
    ) {
    }
}
