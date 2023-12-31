<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Rows;

final class LiteralParameter implements QueryParameter
{
    public function __construct(
        public readonly string $queryParamName,
        public readonly mixed $value,
        public readonly ?int $type = null
    ) {
    }

    public function queryParamName() : string
    {
        return $this->queryParamName;
    }

    public function toQueryParam(Rows $rows) : mixed
    {
        return $this->value;
    }

    public function type() : ?int
    {
        return $this->type;
    }
}
