<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Decimal;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\Thrift\TimeUnit;

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
        private readonly ?Time $time = null,
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
        return new self(self::DECIMAL, decimal: new Decimal($scale, $precision));
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
            $name = self::MAP;
        }

        if ($logicalType->LIST !== null) {
            $name = self::LIST;
        }

        if ($logicalType->ENUM !== null) {
            $name = self::ENUM;
        }

        if ($logicalType->DECIMAL !== null) {
            $name = self::DECIMAL;
        }

        if ($logicalType->DATE !== null) {
            $name = self::DATE;
        }

        if ($logicalType->TIME !== null) {
            $name = self::TIME;
        }

        if ($logicalType->TIMESTAMP !== null) {
            $name = self::TIMESTAMP;
        }

        if ($logicalType->INTEGER !== null) {
            $name = self::INTEGER;
        }

        if ($logicalType->UNKNOWN !== null) {
            $name = self::UNKNOWN;
        }

        if ($logicalType->JSON !== null) {
            $name = self::JSON;
        }

        if ($logicalType->BSON !== null) {
            $name = self::BSON;
        }

        if ($logicalType->UUID !== null) {
            $name = self::UUID;
        }

        if (null === $name) {
            throw new InvalidArgumentException('Unknown logical type');
        }

        return new self(
            $name,
            timestamp: $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null,
            time: $logicalType->TIME !== null ? Time::fromThrift($logicalType->TIME) : null,
            decimal: $logicalType->DECIMAL !== null ? Decimal::fromThrift($logicalType->DECIMAL) : null
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
        return new self(self::TIME, time: new Time(false, false, true, false));
    }

    public static function timestamp() : self
    {
        return new self(self::TIMESTAMP, timestamp: new Timestamp(false, false, true, false));
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

    public function timeData() : ?Time
    {
        return $this->time;
    }

    public function timestampData() : ?Timestamp
    {
        return $this->timestamp;
    }

    public function toThrift() : \Flow\Parquet\Thrift\LogicalType
    {
        return new \Flow\Parquet\Thrift\LogicalType([
            self::BSON => $this->is(self::BSON) ? new \Flow\Parquet\Thrift\BsonType() : null,
            self::DATE => $this->is(self::DATE) ? new \Flow\Parquet\Thrift\DateType() : null,
            self::DECIMAL => $this->is(self::DECIMAL) ? new \Flow\Parquet\Thrift\DecimalType([
                'scale' => $this->decimalData()?->scale(),
                'precision' => $this->decimalData()?->precision(),
            ]) : null,
            self::ENUM => $this->is(self::ENUM) ? new \Flow\Parquet\Thrift\EnumType() : null,
            self::INTEGER => $this->is(self::INTEGER) ? new \Flow\Parquet\Thrift\IntType() : null,
            self::JSON => $this->is(self::JSON) ? new \Flow\Parquet\Thrift\JsonType() : null,
            self::LIST => $this->is(self::LIST) ? new \Flow\Parquet\Thrift\ListType() : null,
            self::MAP => $this->is(self::MAP) ? new \Flow\Parquet\Thrift\MapType() : null,
            self::STRING => $this->is(self::STRING) ? new \Flow\Parquet\Thrift\StringType() : null,
            self::TIME => $this->is(self::TIME) ? new \Flow\Parquet\Thrift\TimeType([
                'isAdjustedToUTC' => $this->timeData()?->isAdjustedToUTC(),
                'unit' => new TimeUnit([
                    'MILLIS' => $this->timeData()?->millis() ? new \Flow\Parquet\Thrift\MilliSeconds() : null,
                    'MICROS' => $this->timeData()?->micros() ? new \Flow\Parquet\Thrift\MicroSeconds() : null,
                    'NANOS' => $this->timeData()?->nanos() ? new \Flow\Parquet\Thrift\NanoSeconds() : null,
                ]),
            ]) : null,
            self::TIMESTAMP => $this->is(self::TIMESTAMP) ? new \Flow\Parquet\Thrift\TimestampType([
                'isAdjustedToUTC' => $this->timestampData()?->isAdjustedToUTC(),
                'unit' => new TimeUnit([
                    'MILLIS' => $this->timestampData()?->millis() ? new \Flow\Parquet\Thrift\MilliSeconds() : null,
                    'MICROS' => $this->timestampData()?->micros() ? new \Flow\Parquet\Thrift\MicroSeconds() : null,
                    'NANOS' => $this->timestampData()?->nanos() ? new \Flow\Parquet\Thrift\NanoSeconds() : null,
                ]),
            ]) : null,
            self::UNKNOWN => $this->is(self::UNKNOWN) ? new \Flow\Parquet\Thrift\NullType() : null,
            self::UUID => $this->is(self::UUID) ? new \Flow\Parquet\Thrift\UUIDType() : null,
        ]);
    }
}
