<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Rows;

use function is_array;
use function is_scalar;

final readonly class LiteralParameter implements QueryParameter
{
    public function __construct(
        private string $queryParamName,
        private mixed $value,
        private ?ArrayParameterType $type = null,
    ) {}

    public function queryParamName(): string
    {
        return $this->queryParamName;
    }

    public function toQueryParam(Rows $rows): array|bool|float|int|string|null
    {
        if (is_array($this->value)) {
            $result = [];

            // @mago-expect analysis:mixed-assignment
            foreach ($this->value as $key => $item) {
                if (is_scalar($item) || $item === null) {
                    $result[$key] = $item;
                }
            }

            return $result;
        }

        return is_scalar($this->value) || $this->value === null ? $this->value : null;
    }

    public function type(): ?ArrayParameterType
    {
        return $this->type;
    }
}
