<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\ThriftModel\BloomFilterAlgorithm;
use Flow\Parquet\ThriftModel\BloomFilterCompression;
use Flow\Parquet\ThriftModel\BloomFilterHash;
use Flow\Parquet\ThriftModel\BloomFilterHeader;
use Flow\Parquet\ThriftModel\SplitBlockAlgorithm;
use Flow\Parquet\ThriftModel\Uncompressed;
use Flow\Parquet\ThriftModel\XxHash;

use function strlen;

final class BloomFilterBytes
{
    /**
     * A header and bitset Sbbf::serialized() never writes: the header declares $numBytes instead of the bitset's
     * length, or another $algorithm, $hash or $compression.
     */
    public static function of(
        string $bitset,
        ?int $numBytes = null,
        BloomFilterAlgorithm $algorithm = new BloomFilterAlgorithm(['BLOCK' => new SplitBlockAlgorithm()]),
        BloomFilterHash $hash = new BloomFilterHash(['XXHASH' => new XxHash()]),
        BloomFilterCompression $compression = new BloomFilterCompression(['UNCOMPRESSED' => new Uncompressed()]),
    ): string {
        return ThriftBytes::of(new BloomFilterHeader([
            'numBytes' => $numBytes ?? strlen($bitset),
            'algorithm' => $algorithm,
            'hash' => $hash,
            'compression' => $compression,
        ])) . $bitset;
    }
}
