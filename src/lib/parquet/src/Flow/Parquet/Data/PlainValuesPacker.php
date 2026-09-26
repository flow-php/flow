<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryWriter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use RuntimeException;

use function array_filter;
use function array_map;
use function Flow\Parquet\Binary\encode_decimal;
use function Flow\Parquet\Binary\encode_f32;
use function Flow\Parquet\Binary\encode_f64;
use function Flow\Parquet\Binary\encode_i32;
use function Flow\Parquet\Binary\encode_i64;
use function Flow\Parquet\Binary\encode_u32;
use function hex2bin;
use function str_replace;
use function strlen;

final readonly class PlainValuesPacker
{
    public function __construct(
        private BinaryWriter $writer,
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {}

    /**
     * @param array<mixed> $values
     */
    public function packValues(FlatColumn $column, array $values): void
    {
        $values = array_filter($values, static fn(mixed $value) => $value !== null);

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                /** @var array<bool> $values */
                $this->packBooleans($values);

                break;
            case PhysicalType::INT32:
                /** @var array<int> $values */
                $this->packInt32s($values);

                break;
            case PhysicalType::INT64:
                /** @var array<int> $values */
                $this->packInt64s($values);

                break;
            case PhysicalType::FLOAT:
                /** @var array<float> $values */
                $this->packFloats($values);

                break;
            case PhysicalType::DOUBLE:
                /** @var array<float> $values */
                $this->packDoubles($values);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::UUID:
                        /** @var array<string> $values */
                        $this->packUuids($values);

                        break;
                    case LogicalType::DECIMAL:
                        /** @var array<float> $values */
                        $this->packDecimals(
                            $values,
                            (int) $column->typeLength(),
                            (int) $column->logicalType()?->decimalData()?->precision(),
                            (int) $column->logicalType()?->decimalData()?->scale(),
                        );

                        break;
                    default:
                        /** @var array<string> $values */
                        $this->packFixedLenByteArrays($values);

                        break;
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::JSON:
                    case LogicalType::STRING:
                        /** @var array<string> $values */
                        $this->packStrings($values);

                        break;
                    case LogicalType::DECIMAL:
                        $precision = (int) $column->logicalType()?->decimalData()?->precision();
                        $scale = (int) $column->logicalType()?->decimalData()?->scale();

                        /** @var array<float|int> $values */
                        $this->packByteArrays(array_map(static fn(float|int $value): string => encode_decimal(
                            (float) $value,
                            $precision,
                            $scale,
                            null,
                        ), $values));

                        break;
                    default:
                        /** @var array<string> $values */
                        $this->packByteArrays($values);

                        break;
                }

                break;

            default:
                throw new RuntimeException(
                    'Writing physical type "' . $column->type()->name . '" is not implemented yet',
                );
        }
    }

    /**
     * @param array<bool> $values
     */
    private function packBooleans(array $values): void
    {
        $bits = [];

        foreach ($values as $value) {
            $bits[] = $value ? 1 : 0;
        }
        $this->writer->writeBits($bits);
    }

    /**
     * @param array<string> $values
     */
    private function packByteArrays(array $values): void
    {
        foreach ($values as $value) {
            $this->writer->append(encode_u32($this->byteOrder, [strlen($value)]));
            $this->writer->append($value);
        }
    }

    /**
     * @param array<float> $decimals
     */
    private function packDecimals(array $decimals, int $byteLength, int $precision, int $scale): void
    {
        foreach ($decimals as $decimal) {
            $this->writer->append(encode_decimal($decimal, $precision, $scale, $byteLength));
        }
    }

    /**
     * @param array<float> $doubles
     */
    private function packDoubles(array $doubles): void
    {
        $this->writer->append(encode_f64($this->byteOrder, $doubles));
    }

    /**
     * @param array<string> $values
     */
    private function packFixedLenByteArrays(array $values): void
    {
        foreach ($values as $value) {
            $this->writer->append($value);
        }
    }

    /**
     * @param array<float> $floats
     */
    private function packFloats(array $floats): void
    {
        $this->writer->append(encode_f32($this->byteOrder, $floats));
    }

    /**
     * @param array<int> $ints
     */
    private function packInt32s(array $ints): void
    {
        $this->writer->append(encode_i32($this->byteOrder, $ints));
    }

    /**
     * @param array<int> $ints
     */
    private function packInt64s(array $ints): void
    {
        $this->writer->append(encode_i64($this->byteOrder, $ints));
    }

    /**
     * @param array<string> $strings
     */
    private function packStrings(array $strings): void
    {
        foreach ($strings as $string) {
            $this->writer->append(encode_u32($this->byteOrder, [strlen($string)]));
            $this->writer->append($string);
        }
    }

    /**
     * @param array<string> $uuids
     */
    private function packUuids(array $uuids): void
    {
        foreach ($uuids as $uuid) {
            $hex = str_replace('-', '', $uuid);
            $binary = hex2bin($hex);

            if ($binary === false) {
                throw new RuntimeException('Invalid UUID format: ' . $uuid);
            }

            $this->writer->append($binary);
        }
    }
}
