<?php

declare(strict_types=1);

namespace Flow\Parquet\BloomFilter;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function Flow\Parquet\Binary\decode_i64;
use function hash;

final readonly class BloomHash
{
    public function __construct(
        private PhysicalCodec $codec = new PhysicalCodec(),
    ) {}

    /**
     * xxh64 (seed 0) of the PLAIN encoding, without BYTE_ARRAY's length prefix; the u64 as a signed PHP int.
     */
    public function of(FlatColumn $column, mixed $physical): int
    {
        return decode_i64(ByteOrder::BIG_ENDIAN, hash(
            'xxh64',
            $this->codec->encodeStatistic($column, $physical),
            true,
        ))[0];
    }
}
