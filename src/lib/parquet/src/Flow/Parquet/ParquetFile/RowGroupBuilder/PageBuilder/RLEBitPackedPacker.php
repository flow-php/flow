<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\{BitWidth, RLEBitPackedHybrid};

final readonly class RLEBitPackedPacker
{
    public function __construct(
        private RLEBitPackedHybrid $bitPackedHybrid,
    ) {
    }

    /**
     * @param array<int> $values
     */
    public function pack(int $bitWidth, array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $bitWidth, $values);

        return $dataBuffer;
    }

    /**
     * @param array<int> $values
     */
    public function packWithBitWidth(int $bitWidth, array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $bitWidth, $values);
        $outputBuffer = '';
        $outputWriter = new BinaryBufferWriter($outputBuffer);
        $outputWriter->writeVarInts32([BitWidth::fromArray($values)]);
        $outputWriter->append($dataBuffer);

        return $outputBuffer;
    }

    /**
     * @param array<int> $values
     */
    public function packWithLength(int $bitWidth, array $values) : string
    {
        $dataBuffer = '';
        $this->bitPackedHybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $bitWidth, $values);
        $outputBuffer = '';
        $outputWriter = new BinaryBufferWriter($outputBuffer);
        $outputWriter->writeInts32([$length = \strlen($dataBuffer)]);
        $outputWriter->append($dataBuffer);

        return $outputBuffer;
    }
}
