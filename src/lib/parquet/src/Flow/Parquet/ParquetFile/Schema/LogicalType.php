<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\LogicalType\Decimal;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ThriftModel\BsonType;
use Flow\Parquet\ThriftModel\DateType;
use Flow\Parquet\ThriftModel\DecimalType;
use Flow\Parquet\ThriftModel\EnumType;
use Flow\Parquet\ThriftModel\Float16Type;
use Flow\Parquet\ThriftModel\IntType;
use Flow\Parquet\ThriftModel\JsonType;
use Flow\Parquet\ThriftModel\ListType;
use Flow\Parquet\ThriftModel\LogicalType as ThriftLogicalType;
use Flow\Parquet\ThriftModel\MapType;
use Flow\Parquet\ThriftModel\MicroSeconds;
use Flow\Parquet\ThriftModel\MilliSeconds;
use Flow\Parquet\ThriftModel\NanoSeconds;
use Flow\Parquet\ThriftModel\NullType;
use Flow\Parquet\ThriftModel\StringType;
use Flow\Parquet\ThriftModel\TimestampType;
use Flow\Parquet\ThriftModel\TimeType;
use Flow\Parquet\ThriftModel\TimeUnit;
use Flow\Parquet\ThriftModel\UUIDType;

final readonly class LogicalType
{
    public const string BSON = 'BSON';

    public const string DATE = 'DATE';

    public const string DECIMAL = 'DECIMAL';

    public const string ENUM = 'ENUM';

    public const string FLOAT16 = 'FLOAT16';

    public const string INTEGER = 'INTEGER';

    public const string JSON = 'JSON';

    public const string LIST = 'LIST';

    public const string MAP = 'MAP';

    public const string STRING = 'STRING';

    public const string TIME = 'TIME';

    public const string TIMESTAMP = 'TIMESTAMP';

    public const string UNKNOWN = 'UNKNOWN';

    public const string UUID = 'UUID';

    public function __construct(
        private string $name,
        private ?Timestamp $timestamp = null,
        private ?Time $time = null,
        private ?Decimal $decimal = null,
    ) {}

    public static function bson(): self
    {
        return new self(self::BSON);
    }

    public static function date(): self
    {
        return new self(self::DATE);
    }

    public static function decimal(int $scale, int $precision): self
    {
        return new self(self::DECIMAL, decimal: new Decimal($scale, $precision));
    }

    public static function enum(): self
    {
        return new self(self::ENUM);
    }

    public static function fromThrift(ThriftLogicalType $logicalType): ?self
    {
        $name = null;

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->STRING !== null) {
            $name = self::STRING;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->MAP !== null) {
            $name = self::MAP;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->LIST !== null) {
            $name = self::LIST;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->ENUM !== null) {
            $name = self::ENUM;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->DECIMAL !== null) {
            $name = self::DECIMAL;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->DATE !== null) {
            $name = self::DATE;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->TIME !== null) {
            $name = self::TIME;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->TIMESTAMP !== null) {
            $name = self::TIMESTAMP;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->INTEGER !== null) {
            $name = self::INTEGER;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->UNKNOWN !== null) {
            $name = self::UNKNOWN;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->JSON !== null) {
            $name = self::JSON;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->BSON !== null) {
            $name = self::BSON;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->UUID !== null) {
            $name = self::UUID;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($logicalType->FLOAT16 !== null) {
            $name = self::FLOAT16;
        }

        // @mago-ignore analysis:impossible-condition
        // @mago-ignore analysis:redundant-comparison
        if (null === $name) {
            return null;
        }

        return new self(
            $name,
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            timestamp: $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null,
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            time: $logicalType->TIME !== null ? Time::fromThrift($logicalType->TIME) : null,
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            decimal: $logicalType->DECIMAL !== null ? Decimal::fromThrift($logicalType->DECIMAL) : null,
        );
    }

    public static function integer(): self
    {
        return new self(self::INTEGER);
    }

    public static function json(): self
    {
        return new self(self::JSON);
    }

    public static function list(): self
    {
        return new self(self::LIST);
    }

    public static function map(): self
    {
        return new self(self::MAP);
    }

    public static function string(): self
    {
        return new self(self::STRING);
    }

    public static function time(): self
    {
        return new self(self::TIME, time: new Time(false, false, true, false));
    }

    public static function timestamp(): self
    {
        return new self(self::TIMESTAMP, timestamp: new Timestamp(false, false, true, false));
    }

    public static function unknown(): self
    {
        return new self(self::UNKNOWN);
    }

    public static function uuid(): self
    {
        return new self(self::UUID);
    }

    public function decimalData(): ?Decimal
    {
        return $this->decimal;
    }

    public function is(string $logicalType): bool
    {
        return $this->name() === $logicalType;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function timeData(): ?Time
    {
        return $this->time;
    }

    public function timestampData(): ?Timestamp
    {
        return $this->timestamp;
    }

    public function toThrift(): ThriftLogicalType
    {
        return new ThriftLogicalType([
            self::BSON => $this->is(self::BSON) ? new BsonType() : null,
            self::DATE => $this->is(self::DATE) ? new DateType() : null,
            self::DECIMAL => $this->is(self::DECIMAL)
                ? new DecimalType([
                    'scale' => $this->decimalData()?->scale(),
                    'precision' => $this->decimalData()?->precision(),
                ]) : null,
            self::ENUM => $this->is(self::ENUM) ? new EnumType() : null,
            self::INTEGER => $this->is(self::INTEGER) ? new IntType() : null,
            self::JSON => $this->is(self::JSON) ? new JsonType() : null,
            self::LIST => $this->is(self::LIST) ? new ListType() : null,
            self::MAP => $this->is(self::MAP) ? new MapType() : null,
            self::STRING => $this->is(self::STRING) ? new StringType() : null,
            self::TIME => $this->is(self::TIME)
                ? new TimeType([
                    'isAdjustedToUTC' => $this->timeData()?->isAdjustedToUTC(),
                    'unit' => new TimeUnit([
                        'MILLIS' => $this->timeData()?->millis() ? new MilliSeconds() : null,
                        'MICROS' => $this->timeData()?->micros() ? new MicroSeconds() : null,
                        'NANOS' => $this->timeData()?->nanos() ? new NanoSeconds() : null,
                    ]),
                ]) : null,
            self::TIMESTAMP => $this->is(self::TIMESTAMP)
                ? new TimestampType([
                    'isAdjustedToUTC' => $this->timestampData()?->isAdjustedToUTC(),
                    'unit' => new TimeUnit([
                        'MILLIS' => $this->timestampData()?->millis() ? new MilliSeconds() : null,
                        'MICROS' => $this->timestampData()?->micros() ? new MicroSeconds() : null,
                        'NANOS' => $this->timestampData()?->nanos() ? new NanoSeconds() : null,
                    ]),
                ]) : null,
            self::UNKNOWN => $this->is(self::UNKNOWN) ? new NullType() : null,
            self::UUID => $this->is(self::UUID) ? new UUIDType() : null,
            self::FLOAT16 => $this->is(self::FLOAT16) ? new Float16Type() : null,
        ]);
    }
}
