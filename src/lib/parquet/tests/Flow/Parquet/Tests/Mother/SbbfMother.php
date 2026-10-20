<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\BloomFilter\SbbfBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class SbbfMother
{
    /**
     * A filter holding the physical values of the column.
     */
    public static function with(FlatColumn $column, bool|float|int|string ...$values): Sbbf
    {
        $builder = SbbfBuilder::for(new BloomFilterOptions(0.01, 100));

        foreach ($values as $value) {
            $builder->insert((new BloomHash())->of($column, $value));
        }

        return $builder->build();
    }
}
