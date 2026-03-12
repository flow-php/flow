<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder;

use function Flow\Parquet\Binary\encode_i32;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\{BitWidth, RLEBitPackedHybrid};

final readonly class RLEBitPackedPacker
{
    public function __construct(
        private RLEBitPackedHybrid $bitPackedHybrid,
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
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
        $outputWriter->writeVarInts([BitWidth::fromArray($values)]);
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
        $outputWriter->append(encode_i32($this->byteOrder, \strlen($dataBuffer)));
        $outputWriter->append($dataBuffer);

        return $outputBuffer;
    }
}
