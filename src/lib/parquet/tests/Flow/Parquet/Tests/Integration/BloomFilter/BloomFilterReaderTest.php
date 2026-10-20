<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\BloomFilter;

use Flow\Parquet\BloomFilter\BloomFilterReader;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\FilterRows;
use PHPUnit\Framework\TestCase;

use function array_slice;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class BloomFilterReaderTest extends TestCase
{
    public function test_every_value_duckdb_wrote_is_in_its_row_group_filter(): void
    {
        $fixture = __DIR__ . '/../Engine/Fixtures/Filters/bloom_duckdb.parquet';
        $metadata = Reader::php()->read($fixture)->metadata();
        $reader = new BloomFilterReader(native_local_filesystem()->readFrom(path($fixture)));
        $rows = FilterRows::bloomRows(FilterRows::DUCKDB_BLOOM_ROWS);
        $first = 0;
        $missing = [];

        foreach ($metadata->rowGroups()->all() as $rowGroup) {
            foreach ($rowGroup->columnChunks() as $chunk) {
                $column = $metadata->schema()->getFlat($chunk->flatPath());
                $bloomFilter = $reader->read($chunk);

                static::assertNotNull($bloomFilter);

                foreach (array_slice($rows, $first, $rowGroup->rowsCount()) as $row) {
                    if (!$bloomFilter->check((new BloomHash())->of($column, $row[$column->name()]))) {
                        $missing[] = $row[$column->name()];
                    }
                }
            }

            $first += $rowGroup->rowsCount();
        }

        static::assertSame(FilterRows::DUCKDB_BLOOM_ROWS, $first);
        static::assertSame([], $missing);
    }
}
