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

    public function toQueryParam(Rows $rows) : array|bool|float|int|string|null
    {
        if (\is_array($this->value)) {
            return \array_filter($this->value, fn ($item) => \is_scalar($item) || $item === null);
        }

        return \is_scalar($this->value) || $this->value === null ? $this->value : null;
    }

    public function type() : int|ArrayParameterType|null
    {
        return $this->type;
    }
}
