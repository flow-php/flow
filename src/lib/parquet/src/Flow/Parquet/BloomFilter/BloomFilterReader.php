<?php

declare(strict_types=1);

namespace Flow\Parquet\BloomFilter;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\BloomFilterHeader;

use function strlen;
use function substr;

final readonly class BloomFilterReader
{
    /**
     * arrow-rs SBBF_HEADER_SIZE_ESTIMATE: read when the chunk does not record the filter's length.
     */
    private const int HEADER_SIZE_ESTIMATE = 20;

    public function __construct(
        private SourceStream $stream,
    ) {}

    /**
     * Null when the chunk has no bloom filter, records an offset no filter can be read from, or holds one that is not a
     * split block, xxh64, uncompressed filter.
     */
    public function read(ColumnChunk $chunk): ?Sbbf
    {
        $offset = $chunk->bloomFilterOffset();

        if ($offset === null || $offset < 0) {
            return null;
        }

        $length = $chunk->bloomFilterLength();

        if ($length !== null && $length < 1) {
            $length = null;
        }

        $bytes = $this->stream->read($length ?? self::HEADER_SIZE_ESTIMATE, $offset);
        $buffer = new MemoryBuffer($bytes);
        $header = new BloomFilterHeader();
        $header->read(new CompactProtocol($buffer));

        if (!self::supported($header)) {
            return null;
        }

        $numBytes = (int) $header->numBytes;

        if ($numBytes < 1) {
            throw new RuntimeException('The bloom filter at offset ' . $offset . ' declares ' . $numBytes . ' bytes');
        }

        $headerSize = strlen($bytes) - $buffer->available();
        $bitset = $length === null
            ? $this->stream->read($numBytes, $offset + $headerSize)
            : substr($bytes, $headerSize, $numBytes);

        if (strlen($bitset) !== $numBytes) {
            throw new RuntimeException(
                'The bloom filter at offset '
                . $offset
                . ' declares '
                . $numBytes
                . ' bytes, '
                . strlen($bitset)
                . ' were read',
            );
        }

        return Sbbf::fromBytes($bitset);
    }

    private static function supported(BloomFilterHeader $header): bool
    {
        return (
            $header->algorithm->BLOCK !== null
            && $header->hash->XXHASH !== null
            && $header->compression->UNCOMPRESSED !== null
        );
    }
}
