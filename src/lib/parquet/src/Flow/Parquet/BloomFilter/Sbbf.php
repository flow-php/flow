<?php

declare(strict_types=1);

namespace Flow\Parquet\BloomFilter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\BloomFilterAlgorithm;
use Flow\Parquet\ThriftModel\BloomFilterCompression;
use Flow\Parquet\ThriftModel\BloomFilterHash;
use Flow\Parquet\ThriftModel\BloomFilterHeader;
use Flow\Parquet\ThriftModel\SplitBlockAlgorithm;
use Flow\Parquet\ThriftModel\Uncompressed;
use Flow\Parquet\ThriftModel\XxHash;

use function intdiv;
use function ord;
use function strlen;

/**
 * A split block bloom filter: blocks of eight little-endian u32 words, one bit set per word for each value.
 */
final readonly class Sbbf
{
    public const int BLOCK_BYTES = 32;

    private const array SALT = [
        0x47B6137B,
        0x44974D91,
        0x8824AD5B,
        0xA2B7289D,
        0x705495C7,
        0x2DF1424B,
        0x9EFC4947,
        0x5C6BFB31,
    ];

    private int $blocks;

    private function __construct(
        private string $bitset,
    ) {
        $this->blocks = intdiv(strlen($bitset), self::BLOCK_BYTES);
    }

    public static function fromBytes(string $bitset): self
    {
        $length = strlen($bitset);

        if ($length < self::BLOCK_BYTES || ($length % self::BLOCK_BYTES) !== 0) {
            throw new InvalidArgumentException(
                'A bloom filter bitset holds whole blocks of '
                . self::BLOCK_BYTES
                . ' bytes, got '
                . $length
                . ' bytes',
            );
        }

        return new self($bitset);
    }

    /**
     * The eight bits a hash sets in a bitset of $blocks blocks, numbered from the first byte's lowest bit. $hash is the
     * xxh64 u64 as a signed PHP int, so every shift is masked and the 32-bit multiply is split in 16-bit halves.
     *
     * @return list<int>
     */
    public static function bits(int $hash, int $blocks): array
    {
        $block = ((($hash >> 32) & 0xFFFFFFFF) * $blocks) >> 32;
        $x = $hash & 0xFFFFFFFF;
        $bits = [];

        foreach (self::SALT as $word => $salt) {
            $product = ((((($x >> 16) * $salt) & 0xFFFF) << 16) + (($x & 0xFFFF) * $salt)) & 0xFFFFFFFF;
            $bits[] = ($block * self::BLOCK_BYTES * 8) + ($word * 32) + ($product >> 27);
        }

        return $bits;
    }

    /**
     * As a file holds it, the form BloomFilterReader reads: the thrift header, then the bitset.
     */
    public function serialized(): string
    {
        (new BloomFilterHeader([
            'numBytes' => strlen($this->bitset),
            'algorithm' => new BloomFilterAlgorithm(['BLOCK' => new SplitBlockAlgorithm()]),
            'hash' => new BloomFilterHash(['XXHASH' => new XxHash()]),
            'compression' => new BloomFilterCompression(['UNCOMPRESSED' => new Uncompressed()]),
        ]))->write(new CompactProtocol($header = new MemoryBuffer()));

        return $header->data() . $this->bitset;
    }

    /**
     * False only when the value was never inserted.
     */
    public function check(int $hash): bool
    {
        foreach (self::bits($hash, $this->blocks) as $bit) {
            if ((ord($this->bitset[$bit >> 3]) & (1 << ($bit & 7))) === 0) {
                return false;
            }
        }

        return true;
    }
}
