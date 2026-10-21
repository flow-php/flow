<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_map;
use function implode;
use function in_array;

final readonly class ColumnEncodingPolicy
{
    private const array SUPPORTED = [
        Encodings::PLAIN,
        Encodings::RLE,
        Encodings::RLE_DICTIONARY,
        Encodings::DELTA_BINARY_PACKED,
        Encodings::DELTA_LENGTH_BYTE_ARRAY,
        Encodings::DELTA_BYTE_ARRAY,
    ];

    public function __construct(
        private int $writerVersion,
        private bool $dictionaryEnabled = true,
    ) {}

    public function encodingFor(FlatColumn $column, ?Encodings $requested = null): Encodings
    {
        if ($requested === null) {
            return $this->dictionaryEnabled && $this->supportsDictionary($column->type())
                ? Encodings::RLE_DICTIONARY
                : $this->fallbackEncoding($column->type());
        }

        $this->assertAllowed($column, $requested);

        return $requested;
    }

    public function fallbackEncoding(PhysicalType $type): Encodings
    {
        return match (true) {
            $this->writerVersion !== 2 => Encodings::PLAIN,
            $type === PhysicalType::BOOLEAN => Encodings::RLE,
            $type === PhysicalType::INT32, $type === PhysicalType::INT64 => Encodings::DELTA_BINARY_PACKED,
            $type === PhysicalType::BYTE_ARRAY,
            $type === PhysicalType::FIXED_LEN_BYTE_ARRAY,
                => Encodings::DELTA_BYTE_ARRAY,
            default => Encodings::PLAIN,
        };
    }

    public function supportsDictionary(PhysicalType $type): bool
    {
        return match ($type) {
            PhysicalType::BOOLEAN => false,
            PhysicalType::FIXED_LEN_BYTE_ARRAY => $this->writerVersion === 2,
            default => true,
        };
    }

    private function assertAllowed(FlatColumn $column, Encodings $encoding): void
    {
        $type = $column->type();

        if (!in_array($encoding, self::SUPPORTED, true)) {
            throw new InvalidArgumentException(
                "Encoding '{$encoding->name}' is not implemented. Supported encodings: "
                    . implode(', ', array_map(
                        static fn(Encodings $supported): string => $supported->name,
                        self::SUPPORTED,
                    )),
            );
        }

        $requirement = match ($encoding) {
            Encodings::RLE => $type === PhysicalType::BOOLEAN ? null : 'is only supported for BOOLEAN columns',
            Encodings::RLE_DICTIONARY => match (true) {
                $this->supportsDictionary($type) => null,
                $type === PhysicalType::FIXED_LEN_BYTE_ARRAY
                    => 'is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1',
                default => "is not supported for {$type->name} columns",
            },
            Encodings::DELTA_BINARY_PACKED => in_array($type, [PhysicalType::INT32, PhysicalType::INT64], true)
                ? null
                : 'is only supported for INT32 and INT64 columns',
            Encodings::DELTA_LENGTH_BYTE_ARRAY => $type === PhysicalType::BYTE_ARRAY
                ? null
                : 'is only supported for BYTE_ARRAY columns',
            Encodings::DELTA_BYTE_ARRAY => in_array(
                $type,
                [PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY],
                true,
            )
                    ? null
                    : 'is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns',
            default => null,
        };

        if ($requirement !== null) {
            throw new InvalidArgumentException(
                "{$encoding->name} encoding {$requirement}. Column '{$column->flatPath()}' has type: {$type->name}",
            );
        }
    }
}
