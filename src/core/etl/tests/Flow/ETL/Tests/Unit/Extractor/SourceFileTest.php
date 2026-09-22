<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\SourceFile;
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
}
