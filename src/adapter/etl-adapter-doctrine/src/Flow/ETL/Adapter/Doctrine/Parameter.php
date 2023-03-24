<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{query_param_name: string, ref: EntryReference, type: int}>
 */
final class Parameter implements Serializable
{
    public function __construct(
        public readonly string $queryParamName,
        public readonly EntryReference $ref,
        public readonly int $type = ArrayParameterType::STRING
    ) {
    }

    public static function asciis(string $queryParamName, EntryReference $ref) : self
    {
        return new self($queryParamName, $ref, ArrayParameterType::ASCII);
    }

    public static function ints(string $queryParamName, EntryReference $ref) : self
    {
        return new self($queryParamName, $ref, ArrayParameterType::INTEGER);
    }

    public static function strings(string $queryParamName, EntryReference $ref) : self
    {
        return new self($queryParamName, $ref, ArrayParameterType::STRING);
    }

    public function __serialize() : array
    {
        return [
            'query_param_name' => $this->queryParamName,
            'ref' => $this->ref,
            'type' => $this->type,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->queryParamName = $data['query_param_name'];
        $this->ref = $data['ref'];
        $this->type = $data['type'];
    }

    public function toQueryParam(Rows $rows) : array
    {
        return $rows->reduceToArray($this->ref);
    }
}
