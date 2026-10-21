<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\RuntimeException;

use function strlen;

final readonly class DeltaLengthByteArrayDecoder
{
    public function __construct(
        private DeltaBinaryPackedDecoder $lengths = new DeltaBinaryPackedDecoder(),
    ) {}

    /**
     * @return list<string>
     */
    public function decode(BinaryBufferReader $reader, int $valueCount): array
    {
        $values = [];

        foreach ($this->lengths->decode($reader, $valueCount) as $index => $length) {
            if ($length < 0) {
                throw new RuntimeException("Invalid DELTA_LENGTH_BYTE_ARRAY length {$length} of value {$index}");
            }

            $value = $reader->readBytes($length);

            if (strlen($value) !== $length) {
                throw new RuntimeException("DELTA_LENGTH_BYTE_ARRAY data ends before value {$index}");
            }

            $values[] = $value;
        }

        return $values;
    }
}
