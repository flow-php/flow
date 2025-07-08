<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\EfficientRowGroupBuilder\ValueStorage;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class BufferValueStorage implements ValueStorage
{
    private string $buffer = '';

    public function addValues(FlatColumn $column, array $values) : void
    {
        $localBuffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($localBuffer)))->packValues($column, $values);
        $this->buffer .= $localBuffer;
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
    }

    public function size() : int
    {
        return \strlen($this->buffer);
    }
}
