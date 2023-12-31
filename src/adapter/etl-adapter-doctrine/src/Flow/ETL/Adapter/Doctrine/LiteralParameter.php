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

    public function __serialize() : array
    {
        return [
            'query_param_name' => $this->queryParamName,
            'value' => $this->value,
            'type' => $this->type,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->queryParamName = $data['query_param_name'];
        $this->value = $data['value'];
        $this->type = $data['type'];
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
