<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\LogicalType\Decimal;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ThriftModel\BsonType;
use Flow\Parquet\ThriftModel\DateType;
use Flow\Parquet\ThriftModel\DecimalType;
use Flow\Parquet\ThriftModel\EnumType;
use Flow\Parquet\ThriftModel\Float16Type;
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
use Flow\Parquet\ThriftModel\TimeUnit as ThriftTimeUnit;
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
        private ?Integer $integer = null,
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

    /**
     * The converted type a writer annotates this logical type with for older readers (arrow-rs 60
     * `ConvertedType::from(Option<LogicalType>)`); null where none exists.
     */
    public function convertedType(): ?ConvertedType
    {
        return match ($this->name) {
            self::STRING => ConvertedType::UTF8,
            self::MAP => ConvertedType::MAP,
            self::LIST => ConvertedType::LIST,
            self::ENUM => ConvertedType::ENUM,
            self::DECIMAL => ConvertedType::DECIMAL,
            self::DATE => ConvertedType::DATE,
            self::TIME => match ($this->time?->unit()) {
                TimeUnit::MILLISECONDS => ConvertedType::TIME_MILLIS,
                TimeUnit::MICROSECONDS => ConvertedType::TIME_MICROS,
                default => null,
            },
            self::TIMESTAMP => match ($this->timestamp?->unit()) {
                TimeUnit::MILLISECONDS => ConvertedType::TIMESTAMP_MILLIS,
                TimeUnit::MICROSECONDS => ConvertedType::TIMESTAMP_MICROS,
                default => null,
            },
            self::INTEGER => match ([$this->integer?->bitWidth(), $this->integer?->isSigned()]) {
                [8, true] => ConvertedType::INT_8,
                [16, true] => ConvertedType::INT_16,
                [32, true] => ConvertedType::INT_32,
                [64, true] => ConvertedType::INT_64,
                [8, false] => ConvertedType::UINT_8,
                [16, false] => ConvertedType::UINT_16,
                [32, false] => ConvertedType::UINT_32,
                [64, false] => ConvertedType::UINT_64,
                default => null,
            },
            self::JSON => ConvertedType::JSON,
            self::BSON => ConvertedType::BSON,
            default => null,
        };
    }

    public static function fromConvertedType(ConvertedType $convertedType, ?int $scale, ?int $precision): ?self
    {
        return match ($convertedType) {
            ConvertedType::DATE => self::date(),
            ConvertedType::DECIMAL => self::decimal($scale ?? 0, $precision ?? 0),
            ConvertedType::TIME_MILLIS => new self(self::TIME, time: new Time(true, TimeUnit::MILLISECONDS)),
            ConvertedType::TIME_MICROS => new self(self::TIME, time: new Time(true, TimeUnit::MICROSECONDS)),
            ConvertedType::TIMESTAMP_MILLIS => new self(
                self::TIMESTAMP,
                timestamp: new Timestamp(true, TimeUnit::MILLISECONDS),
            ),
            ConvertedType::TIMESTAMP_MICROS => new self(
                self::TIMESTAMP,
                timestamp: new Timestamp(true, TimeUnit::MICROSECONDS),
            ),
            default => null,
        };
    }

    public static function fromThrift(ThriftLogicalType $logicalType): ?self
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

        if ($logicalType->FLOAT16 !== null) {
            $name = self::FLOAT16;
        }

        if (null === $name) {
            return null;
        }

        return new self(
            $name,
            timestamp: $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null,
            time: $logicalType->TIME !== null ? Time::fromThrift($logicalType->TIME) : null,
            decimal: $logicalType->DECIMAL !== null ? Decimal::fromThrift($logicalType->DECIMAL) : null,
            integer: $logicalType->INTEGER !== null ? Integer::fromThrift($logicalType->INTEGER) : null,
        );
    }

    public static function integer(int $bitWidth, bool $isSigned): self
    {
        return new self(self::INTEGER, integer: new Integer($bitWidth, $isSigned));
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
        return new self(self::TIME, time: new Time(false, TimeUnit::MICROSECONDS));
    }

    public static function timestamp(): self
    {
        return new self(self::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::MICROSECONDS));
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

    public function integerData(): ?Integer
    {
        return $this->integer;
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
            self::INTEGER => $this->is(self::INTEGER) ? $this->integer?->toThrift() : null,
            self::JSON => $this->is(self::JSON) ? new JsonType() : null,
            self::LIST => $this->is(self::LIST) ? new ListType() : null,
            self::MAP => $this->is(self::MAP) ? new MapType() : null,
            self::STRING => $this->is(self::STRING) ? new StringType() : null,
            self::TIME => $this->is(self::TIME)
                ? new TimeType([
                    'isAdjustedToUTC' => $this->timeData()?->isAdjustedToUTC(),
                    'unit' => new ThriftTimeUnit([
                        'MILLIS' => $this->timeData()?->millis() ? new MilliSeconds() : null,
                        'MICROS' => $this->timeData()?->micros() ? new MicroSeconds() : null,
                        'NANOS' => $this->timeData()?->nanos() ? new NanoSeconds() : null,
                    ]),
                ]) : null,
            self::TIMESTAMP => $this->is(self::TIMESTAMP)
                ? new TimestampType([
                    'isAdjustedToUTC' => $this->timestampData()?->isAdjustedToUTC(),
                    'unit' => new ThriftTimeUnit([
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
