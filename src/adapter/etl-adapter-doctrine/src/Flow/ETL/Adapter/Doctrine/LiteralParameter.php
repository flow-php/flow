<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Rows;

final readonly class LiteralParameter implements QueryParameter
{
    public function __construct(
        private string $queryParamName,
        private mixed $value,
        private int|ArrayParameterType|null $type = null,
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
