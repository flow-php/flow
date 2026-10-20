<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use Flow\Parquet\Engine\Arrow\RustRowGroupChunks;
use Flow\Parquet\Filter\Evaluation\RowGroupZoneMaps;
use Flow\Parquet\Filter\Evaluation\ZoneMap;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ThriftBytes;
use Flow\Parquet\Tests\Double\RowGroupChunksSpy;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use PHPUnit\Framework\TestCase;

use function pack;

use const NAN;

final class RowGroupZoneMapsTest extends TestCase
{
    public function test_a_zone_is_the_chunk_statistics_under_the_column_order(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();
        $id = $metadata->schema()->getFlat('id');

        static::assertEquals(
            new ZoneMap(3, 4, true, true, 0, null, 2, false),
            (new RowGroupZoneMaps(new MetadataRowGroupChunks($metadata), 1))->zone($id),
        );
    }

    public function test_bounds_an_order_does_not_trust_are_absent(): void
    {
        $id = FlatColumn::int64('id');
        $chunk = ColumnChunkMother::withStatistics(
            $id,
            new Statistics(null, null, 1, null, "\x01\x00\x00\x00\x00\x00\x00\x00", "\x01\x00\x00\x00\x00\x00\x00\x00"),
        );

        static::assertEquals(
            new ZoneMap(null, null, true, true, 1, null, 2, false),
            (new RowGroupZoneMaps(
                RustRowGroupChunks::fromThrift([2], ['id' => [ThriftBytes::of($chunk->toThrift())]], ['id' => null]),
                0,
            ))->zone($id),
        );
    }

    public function test_bytes_bounds_without_exactness_flags_are_inexact(): void
    {
        $name = FlatColumn::string('name');
        $chunk = ColumnChunkMother::withStatistics($name, new Statistics(null, null, 0, null, 'b', 'a'));
        $zone = (new RowGroupZoneMaps(
            RustRowGroupChunks::fromThrift(
                [2],
                ['name' => [ThriftBytes::of($chunk->toThrift())]],
                ['name' => ThriftBytes::of(ColumnOrder::TypeDefined->toThrift())],
            ),
            0,
        ))->zone($name);

        static::assertEquals(new ZoneMap('a', 'b', false, false, 0, null, 2, false), $zone);
    }

    public function test_a_chunk_without_statistics_has_no_zone(): void
    {
        $id = FlatColumn::int64('id');
        $chunk = ColumnChunkMother::withStatistics($id, null);

        static::assertNull((new RowGroupZoneMaps(
            RustRowGroupChunks::fromThrift([2], ['id' => [ThriftBytes::of($chunk->toThrift())]], ['id' => null]),
            0,
        ))->zone($id));
    }

    public function test_deprecated_bounds_are_never_used_even_under_a_trusted_order(): void
    {
        $name = FlatColumn::string('name');
        $chunk = ColumnChunkMother::withStatistics($name, new Statistics('z', 'a', 0, null, null, null));

        static::assertEquals(
            new ZoneMap(null, null, false, false, 0, null, 2, false),
            (new RowGroupZoneMaps(
                RustRowGroupChunks::fromThrift(
                    [2],
                    ['name' => [ThriftBytes::of($chunk->toThrift())]],
                    ['name' => ThriftBytes::of(ColumnOrder::TypeDefined->toThrift())],
                ),
                0,
            ))->zone($name),
        );
    }

    public function test_a_nan_bound_under_total_order_makes_the_zone_all_nan(): void
    {
        $score = FlatColumn::double('score');
        $chunk = ColumnChunkMother::withStatistics(
            $score,
            new Statistics(null, null, 0, null, pack('e', NAN), pack('e', NAN), nanCount: 2),
        );
        $chunks = static fn(ColumnOrder $order): RustRowGroupChunks => RustRowGroupChunks::fromThrift(
            [2],
            ['score' => [ThriftBytes::of($chunk->toThrift())]],
            ['score' => ThriftBytes::of($order->toThrift())],
        );

        static::assertTrue((new RowGroupZoneMaps($chunks(ColumnOrder::Ieee754TotalOrder), 0))->zone($score)?->allNaN);
        static::assertEquals(
            new ZoneMap(null, null, true, true, 0, 2, 2, false),
            (new RowGroupZoneMaps($chunks(ColumnOrder::TypeDefined), 0))->zone($score),
        );
    }

    public function test_a_zone_is_decoded_once_per_column(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();
        $chunks = new RowGroupChunksSpy(new MetadataRowGroupChunks($metadata));
        $zones = new RowGroupZoneMaps($chunks, 2);

        static::assertSame(
            $zones->zone($metadata->schema()->getFlat('id')),
            $zones->zone($metadata->schema()->getFlat('id')),
        );
        static::assertSame([2], $chunks->chunkReads);
    }
}
