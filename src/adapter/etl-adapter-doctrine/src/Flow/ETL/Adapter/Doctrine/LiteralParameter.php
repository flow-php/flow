<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Rows;

final class LiteralParameter implements QueryParameter
{
    public function __construct(
        private readonly string $queryParamName,
        private readonly mixed $value,
        private readonly int|ArrayParameterType|null $type = null
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

    public function type() : int|ArrayParameterType|null
    {
        return $this->type;
    }
}
