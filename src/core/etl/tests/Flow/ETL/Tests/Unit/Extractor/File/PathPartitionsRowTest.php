<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Extractor\File\PathPartitionsRow;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FileStatus;

use function Flow\Filesystem\DSL\path;

final class PathPartitionsRowTest extends FlowTestCase
{
    public function test_a_file_is_described_by_its_uri_and_its_partitions(): void
    {
        static::assertSame(
            ['path' => 'memory://dir/year=2024/month=01/a.csv', 'partitions' => ['year' => '2024', 'month' => '01']],
            (new PathPartitionsRow())->row(new FileStatus(path('memory://dir/year=2024/month=01/a.csv'), true)),
        );
    }

    public function test_a_file_outside_partitions_has_none(): void
    {
        static::assertSame(
            [],
            (new PathPartitionsRow())->row(new FileStatus(path('memory://dir/a.csv'), true))['partitions'],
        );
    }
}
