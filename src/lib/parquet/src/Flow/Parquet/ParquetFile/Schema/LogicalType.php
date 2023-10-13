<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;

final class LogicalType
{
    public const BSON = 'BSON';

    public const DATE = 'DATE';

    public const DECIMAL = 'DECIMAL';

    public const ENUM = 'ENUM';

    public const INTEGER = 'INTEGER';

    public const JSON = 'JSON';

    public const LIST = 'LIST';

    public const MAP = 'MAP';

    public const STRING = 'STRING';

    public const TIME = 'TIME';

    public const TIMESTAMP = 'TIMESTAMP';

    public const UNKNOWN = 'UNKNOWN';

    public const UUID = 'UUID';

    public function __construct(private readonly string $name, private readonly ?Timestamp $timestamp = null)
    {
    }

    /**
     * @psalm-suppress DocblockTypeContradiction
     * @psalm-suppress RedundantConditionGivenDocblockType
     */
    public static function fromThrift(\Flow\Parquet\Thrift\LogicalType $logicalType) : self
    {
        $name = null;

        if ($logicalType->STRING !== null) {
            $name = self::STRING;
        }

        if ($logicalType->MAP !== null) {
            $name =  self::MAP;
        }

        if ($logicalType->LIST !== null) {
            $name =  self::LIST;
        }

        if ($logicalType->ENUM !== null) {
            $name =  self::ENUM;
        }

        if ($logicalType->DECIMAL !== null) {
            $name =  self::DECIMAL;
        }

        if ($logicalType->DATE !== null) {
            $name =  self::DATE;
        }

        if ($logicalType->TIME !== null) {
            $name =  self::TIME;
        }

        if ($logicalType->TIMESTAMP !== null) {
            $name =  self::TIMESTAMP;
        }

        if ($logicalType->INTEGER !== null) {
            $name =  self::INTEGER;
        }

        if ($logicalType->UNKNOWN !== null) {
            $name = self::UNKNOWN;
        }

        if ($logicalType->JSON !== null) {
            $name = self::JSON;
        }

        if ($logicalType->BSON !== null) {
            $name =  self::BSON;
        }

        if ($logicalType->UUID !== null) {
            $name =  self::UUID;
        }

        if (null === $name) {
            throw new InvalidArgumentException('Unknown logical type');
        }

        return new self(
            $name,
            $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null
        );
    }

    public function is(string $logicalType) : bool
    {
        return $this->name() === $logicalType;
    }

    /**
     * @psalm-suppress RedundantConditionGivenDocblockType
     */
    public function name() : string
    {
        return $this->name;
    }

    public function timestamp() : ?Timestamp
    {
        return $this->timestamp;
    }
}
