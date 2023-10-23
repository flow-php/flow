<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Decimal;
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

    public function __construct(
        private readonly string $name,
        private readonly ?Timestamp $timestamp = null,
        private readonly ?Decimal $decimal = null
    ) {
    }

    public static function bson() : self
    {
        return new self(self::BSON);
    }

    public static function date() : self
    {
        return new self(self::DATE);
    }

    public static function decimal(int $scale, int $precision) : self
    {
        return new self(self::DECIMAL, null, new Decimal($scale, $precision));
    }

    public static function enum() : self
    {
        return new self(self::ENUM);
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
            $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null,
            $logicalType->DECIMAL !== null ? Decimal::fromThrift($logicalType->DECIMAL) : null
        );
    }

    public static function integer() : self
    {
        return new self(self::INTEGER);
    }

    public static function json() : self
    {
        return new self(self::JSON);
    }

    public static function list() : self
    {
        return new self(self::LIST);
    }

    public static function map() : self
    {
        return new self(self::MAP);
    }

    public static function string() : self
    {
        return new self(self::STRING);
    }

    public static function time() : self
    {
        return new self(self::TIME);
    }

    public static function timestamp() : self
    {
        return new self(self::TIMESTAMP, new Timestamp(false, false, true, false));
    }

    public static function unknown() : self
    {
        return new self(self::UNKNOWN);
    }

    public function decimalData() : ?Decimal
    {
        return $this->decimal;
    }

    public function is(string $logicalType) : bool
    {
        return $this->name() === $logicalType;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function timestampData() : ?Timestamp
    {
        return $this->timestamp;
    }
}
