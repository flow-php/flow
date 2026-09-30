<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftModel\IntType;

final readonly class Integer
{
    public function __construct(
        private int $bitWidth,
        private bool $isSigned,
    ) {}

    /**
     * The column's integer width and sign: its converted type, else its logical INTEGER; null for neither.
     */
    public static function forColumn(FlatColumn $column): ?self
    {
        return match ($column->convertedType()) {
            ConvertedType::INT_8 => new self(8, true),
            ConvertedType::INT_16 => new self(16, true),
            ConvertedType::INT_32 => new self(32, true),
            ConvertedType::INT_64 => new self(64, true),
            ConvertedType::UINT_8 => new self(8, false),
            ConvertedType::UINT_16 => new self(16, false),
            ConvertedType::UINT_32 => new self(32, false),
            ConvertedType::UINT_64 => new self(64, false),
            default => $column->logicalType()?->integerData(),
        };
    }

    public static function fromThrift(IntType $thrift): self
    {
        return new self((int) $thrift->bitWidth, $thrift->isSigned);
    }

    public function bitWidth(): int
    {
        return $this->bitWidth;
    }

    public function isSigned(): bool
    {
        return $this->isSigned;
    }

    public function toThrift(): IntType
    {
        return new IntType(['bitWidth' => $this->bitWidth, 'isSigned' => $this->isSigned]);
    }
}
