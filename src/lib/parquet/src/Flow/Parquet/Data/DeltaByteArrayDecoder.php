<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\RuntimeException;

use function sprintf;
use function strlen;
use function substr;

final readonly class DeltaByteArrayDecoder
{
    public function __construct(
        private DeltaBinaryPackedDecoder $prefixLengths = new DeltaBinaryPackedDecoder(),
        private DeltaLengthByteArrayDecoder $suffixes = new DeltaLengthByteArrayDecoder(),
    ) {}

    /**
     * @return list<string>
     */
    public function decode(BinaryBufferReader $reader, int $valueCount): array
    {
        $prefixLengths = $this->prefixLengths->decode($reader, $valueCount);
        $values = [];
        $previous = '';

        foreach ($this->suffixes->decode($reader, $valueCount) as $index => $suffix) {
            if ($prefixLengths[$index] < 0 || $prefixLengths[$index] > strlen($previous)) {
                throw new RuntimeException(sprintf(
                    'Invalid DELTA_BYTE_ARRAY prefix length %d of value %d, the previous value has %d bytes',
                    $prefixLengths[$index],
                    $index,
                    strlen($previous),
                ));
            }

            $previous = substr($previous, 0, $prefixLengths[$index]) . $suffix;
            $values[] = $previous;
        }

        return $values;
    }
}
