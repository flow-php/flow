<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\Data\Converter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class BytesStringConverter implements Converter
{
    public function fromParquetType(mixed $data) : string
    {
        return $this->bytesToString($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::BYTE_ARRAY && $column->logicalType() === null && $options->get(Option::BYTE_ARRAY_TO_STRING)) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : Bytes
    {
        return $this->stringToBytes($data);
    }

    private function bytesToString(Bytes $bytes) : string
    {
        return \implode('', \array_map('chr', $bytes->toArray()));
    }

    private function stringToBytes(string $string) : Bytes
    {
        return new Bytes(\array_map('ord', \str_split($string)));
    }
}
