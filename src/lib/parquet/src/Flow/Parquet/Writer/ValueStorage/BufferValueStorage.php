<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class BufferValueStorage implements ValueStorage
{
    private string $buffer = '';

    private readonly ByteOrder $byteOrder;

    private int $size = 0;

    public function __construct()
    {
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
    }

    public function addValues(FlatColumn $column, array $values) : void
    {
        $localBuffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($localBuffer), $this->byteOrder))->packValues($column, $values);
        $this->buffer .= $localBuffer;
        $this->size += \strlen($localBuffer);
    }

    public function getBuffer() : string
    {
        return $this->buffer;
    }

    public function isEmpty() : bool
    {
        return $this->buffer === '';
    }

    public function reset() : void
    {
        $this->buffer = '';
        $this->size = 0;
    }

    public function size() : int
    {
        return $this->size;
    }
}
