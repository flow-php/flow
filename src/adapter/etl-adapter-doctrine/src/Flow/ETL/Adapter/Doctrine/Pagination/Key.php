<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;

final readonly class Key
{
    public function __construct(
        public string $column,
        public Order $order,
        public string|int|ParameterType|Type $type = ParameterType::STRING,
    ) {
    }

    public static function asc(string $column, string|int|ParameterType|Type $type = ParameterType::STRING) : self
    {
        return new self($column, Order::ASC, $type);
    }

    public static function desc(string $column, string|int|ParameterType|Type $type = ParameterType::STRING) : self
    {
        return new self($column, Order::DESC, $type);
    }
}
