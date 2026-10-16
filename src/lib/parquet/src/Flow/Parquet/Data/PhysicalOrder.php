<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function chr;
use function is_float;
use function is_int;
use function is_nan;
use function is_string;
use function ord;
use function pack;
use function str_pad;
use function strcmp;
use function strlen;
use function substr;
use function unpack;

use const PHP_INT_MAX;
use const PHP_INT_MIN;
use const STR_PAD_LEFT;

enum PhysicalOrder
{
    case Boolean;
    case Bytes;
    case Decimal;
    case Double;
    case Float;
    case Float16;
    case Signed;
    case Unordered;
    case Unsigned32;
    case Unsigned64;

    /**
     * Two's complement decimals are sign-extended to this many bytes: a DECIMAL(76) needs 32.
     */
    private const int DECIMAL_KEY_WIDTH = 32;

    public static function forColumn(FlatColumn $column): self
    {
        $logical = $column->logicalType();

        return match ($column->type()) {
            PhysicalType::BOOLEAN => self::Boolean,
            PhysicalType::INT32 => Integer::forColumn($column)?->isSigned() === false ? self::Unsigned32 : self::Signed,
            PhysicalType::INT64 => Integer::forColumn($column)?->isSigned() === false ? self::Unsigned64 : self::Signed,
            PhysicalType::FLOAT => self::Float,
            PhysicalType::DOUBLE => self::Double,
            PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY => match (true) {
                $column->convertedType() === ConvertedType::INTERVAL => self::Unordered,
                $logical?->is(LogicalType::DECIMAL) === true,
                $column->convertedType() === ConvertedType::DECIMAL,
                    => self::Decimal,
                $logical?->is(LogicalType::FLOAT16) === true => self::Float16,
                default => self::Bytes,
            },
            PhysicalType::INT96 => self::Unordered,
        };
    }

    /**
     * Orders two physical values as parquet statistics do: bytes unsigned, DECIMAL signed two's complement, UINT
     * unsigned, floats by IEEE 754 total order (-0.0 < +0.0, NaN greatest), false < true.
     */
    public function compare(bool|float|int|string $a, bool|float|int|string $b): int
    {
        return $this->compareKeys($this->key($a), $this->key($b));
    }

    public function compareKeys(int|string $a, int|string $b): int
    {
        return is_int($a) ? $a <=> $b : strcmp($a, (string) $b) <=> 0;
    }

    public function isFloatingPoint(): bool
    {
        return $this === self::Float || $this === self::Double || $this === self::Float16;
    }

    /**
     * Whether the deprecated statistics min/max may hold the column's bounds (arrow-rs SortOrder::SIGNED).
     */
    public function isSigned(): bool
    {
        return $this === self::Signed || $this === self::Decimal;
    }

    public function isNan(bool|float|int|string $value): bool
    {
        return match ($this) {
            self::Double, self::Float => is_float($value) && is_nan($value),
            self::Float16 => is_string($value)
                && strlen($value) === 2
                && ($this->float16Bits($value) & 0x7FFF) > 0x7C00,
            default => false,
        };
    }

    public function isUnordered(): bool
    {
        return $this === self::Unordered;
    }

    public function isZero(bool|float|int|string $value): bool
    {
        return match ($this) {
            self::Double, self::Float => is_float($value) && $value === 0.0,
            self::Float16 => is_string($value) && strlen($value) === 2 && ($this->float16Bits($value) & 0x7FFF) === 0,
            default => false,
        };
    }

    public function key(bool|float|int|string $value): int|string
    {
        return match ($this) {
            self::Boolean, self::Signed => (int) $value,
            self::Unsigned32 => (int) $value & 0xFFFFFFFF,
            self::Unsigned64 => (int) $value ^ PHP_INT_MIN,
            self::Float => $this->float32Key((float) $value),
            self::Double => $this->float64Key((float) $value),
            self::Float16 => $this->float16Key((string) $value),
            self::Bytes => (string) $value,
            self::Decimal => $this->decimalKey((string) $value),
            self::Unordered => throw new InvalidArgumentException('INT96 and INTERVAL have no physical order'),
        };
    }

    public function zero(bool $negative): float|string
    {
        return match ($this) {
            self::Double, self::Float => $negative ? -0.0 : 0.0,
            self::Float16 => $negative ? "\x00\x80" : "\x00\x00",
            default => throw new InvalidArgumentException($this->name . ' has no signed zero'),
        };
    }

    private function decimalKey(string $bytes): string
    {
        if ($bytes === '') {
            return '';
        }

        $extension = ord($bytes[0]) >= 0x80 ? "\xFF" : "\x00";

        while (strlen($bytes) > self::DECIMAL_KEY_WIDTH && $bytes[0] === $extension && $bytes[1] === $extension) {
            $bytes = substr($bytes, 1);
        }

        $padded = str_pad($bytes, self::DECIMAL_KEY_WIDTH, $extension, STR_PAD_LEFT);

        return chr(ord($padded[0]) ^ 0x80) . substr($padded, 1);
    }

    private function float16Bits(string $bytes): int
    {
        return (ord($bytes[1]) << 8) | ord($bytes[0]);
    }

    private function float16Key(string $bytes): int
    {
        $unsigned = $this->float16Bits($bytes);
        $bits = $unsigned >= 0x8000 ? $unsigned - 0x10000 : $unsigned;

        return $bits ^ (($bits >> 15) & 0x7FFF);
    }

    private function float32Key(float $value): int
    {
        /** @var array{1: int} $unpacked */
        $unpacked = unpack('V', pack('g', $value));
        $bits = $unpacked[1] >= 0x80000000 ? $unpacked[1] - 0x100000000 : $unpacked[1];

        return $bits ^ (($bits >> 31) & 0x7FFFFFFF);
    }

    private function float64Key(float $value): int
    {
        /** @var array{1: int} $unpacked */
        $unpacked = unpack('P', pack('e', $value));

        return $unpacked[1] ^ (($unpacked[1] >> 63) & PHP_INT_MAX);
    }
}
