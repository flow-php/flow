<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Filesystem\DSL\path;

final class SourceFileTest extends FlowTestCase
{
    public function test_a_path_without_partitions_carries_no_values(): void
    {
        static::assertSame([], (new SourceFile(path('memory://orders/data.csv')))->partitionValues);
    }

    public function test_partition_values_come_from_the_path(): void
    {
        static::assertSame(
            ['country' => 'PL', 'year' => '2024'],
            (new SourceFile(path('memory://orders/country=PL/year=2024/data.csv')))->partitionValues,
        );
    }

    public function test_uri_is_the_paths_uri(): void
    {
        static::assertSame(
            'memory://orders/year=2024/data.csv',
            (new SourceFile(path('memory://orders/year=2024/data.csv')))->uri(),
        );
    }

    public function test_it_carries_the_listed_size(): void
    {
        static::assertSame(1024, (new SourceFile(path('memory://orders/data.csv'), 1024))->size);
    }

    public function test_a_size_may_be_absent(): void
    {
        static::assertNull((new SourceFile(path('memory://orders/data.csv')))->size);
    }

    public function test_it_normalizes_and_rebuilds_equal(): void
    {
        $source = new SourceFile(path('memory://orders/year=2026/a.csv'), 12);

        static::assertSame(['path' => $source->path->normalize(), 'size' => 12], $source->normalize());
        static::assertEquals($source, SourceFile::fromArray($source->normalize()));
        static::assertEquals(
            new SourceFile(path('memory://a.csv')),
            SourceFile::fromArray((new SourceFile(path('memory://a.csv')))->normalize()),
        );
    }
}
