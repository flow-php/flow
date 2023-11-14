<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;

final class RLEBitPackedPacker
{
    public function __construct(
        private readonly RLEBitPackedHybrid $bitPackedHybrid
    ) {
    }

    /**
     * @param array<int> $values
     */
    public function pack(array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $values);

        return $dataBuffer;
    }

    /**
     * @param array<int> $values
     */
    public function packWithBitWidth(array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $values);
        $outputBuffer = '';
        $outputWriter = new BinaryBufferWriter($outputBuffer);
        $outputWriter->writeVarInts32([BitWidth::fromArray($values)]);
        $outputWriter->append($dataBuffer);

        return $outputBuffer;
    }

    /**
     * @param array<int> $values
     */
    public function packWithLength(array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $values);
        $outputBuffer = '';
        $outputWriter = new BinaryBufferWriter($outputBuffer);
        $outputWriter->writeInts32([$length = \strlen($dataBuffer)]);
        $outputWriter->append($dataBuffer);

        return $outputBuffer;
    }
}
