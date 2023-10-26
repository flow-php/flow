<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkStatistics;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\TestCase;

final class ColumnChunkSizeStatisticsTest extends TestCase
{
    public function test_int32_statistics() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::int32('int32'));

        for ($i = 0; $i < 100; $i++) {
            $statistics->add($i);
        }

        $this->assertSame(100, $statistics->valuesCount());
        $this->assertSame(0, $statistics->nullCount());
        $this->assertSame(4 * 100, $statistics->uncompressedSize());
    }

    public function test_int64_statistics() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::int64('int64'));

        for ($i = 0; $i < 100; $i++) {
            $statistics->add($i);
        }

        $this->assertSame(100, $statistics->valuesCount());
        $this->assertSame(0, $statistics->nullCount());
        $this->assertSame(8 * 100, $statistics->uncompressedSize());
    }

    public function test_string_statistics() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::string('int64'));

        for ($i = 0; $i < 100; $i++) {
            $statistics->add($string = 'string with a fixed length');
        }

        $this->assertSame(100, $statistics->valuesCount());
        $this->assertSame(0, $statistics->nullCount());
        $this->assertSame(\strlen($string) * $statistics->notNullCount() + (4 * $statistics->notNullCount()), $statistics->uncompressedSize());
    }
}
