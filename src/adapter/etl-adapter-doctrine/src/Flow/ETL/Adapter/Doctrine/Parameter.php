<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{query_param_name: string, ref: EntryReference, type: int}>
 */
final class Parameter implements QueryParameter, Serializable
{
    /**
     * @psalm-suppress DeprecatedConstant
     */
    public function __construct(
        public readonly string $queryParamName,
        public readonly EntryReference $ref,
        public readonly int $type = Connection::PARAM_STR_ARRAY,
    ) {
    }

    public static function asciis(string $queryParamName, EntryReference $ref) : self
    {
        if (!\class_exists('Doctrine\DBAL\ArrayParameterType')) {
            throw new \RuntimeException('Doctrine\DBAL\ArrayParameterType is not available, please upgrade doctrine/dbal to latest version');
        }

        return new self($queryParamName, $ref, ArrayParameterType::ASCII);
    }

    public static function ints(string $queryParamName, EntryReference $ref) : self
    {
        /** @psalm-suppress DeprecatedConstant */
        return new self($queryParamName, $ref, Connection::PARAM_INT_ARRAY);
    }

    public static function strings(string $queryParamName, EntryReference $ref) : self
    {
        /** @psalm-suppress DeprecatedConstant */
        return new self($queryParamName, $ref, Connection::PARAM_STR_ARRAY);
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

    public function queryParamName() : string
    {
        return $this->queryParamName;
    }

    public function toQueryParam(Rows $rows) : mixed
    {
        return $rows->reduceToArray($this->ref);
    }

    public function type() : ?int
    {
        return $this->type;
    }
}
