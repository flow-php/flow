<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

use function is_scalar;

final readonly class Parameter implements QueryParameter
{
    public function __construct(
        private string $queryParamName,
        private Reference $ref,
        private ArrayParameterType $type = ArrayParameterType::STRING,
    ) {}

    public static function asciis(string $queryParamName, Reference $ref): self
    {
        return new self($queryParamName, $ref, ArrayParameterType::ASCII);
    }

    public static function ints(string $queryParamName, Reference $ref): self
    {
        return new self($queryParamName, $ref, ArrayParameterType::INTEGER);
    }

    public static function strings(string $queryParamName, Reference $ref): self
    {
        return new self($queryParamName, $ref, ArrayParameterType::STRING);
    }

    public function queryParamName(): string
    {
        return $this->queryParamName;
    }

    /**
     * @return array<array-key, null|bool|float|int|string>
     */
    public function toQueryParam(Rows $rows): array
    {
        $result = [];

        // @mago-expect analysis:mixed-assignment
        foreach ($rows->reduceToArray($this->ref) as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $result[$key] = $value;
            }
        }

        return $result;
    }

    public function type(): ArrayParameterType
    {
        return $this->type;
    }
}
