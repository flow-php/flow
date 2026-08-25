<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Partition;

use function array_search;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FilesSinkTest extends FlowTestCase
{
    public function test_abandon_closes_every_stream_before_removing_any_file(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $files = new FilesSink($filesystem, path('memory://out.csv'), overwrite());
        $files->writeTo()->append('half written');

        $files->abandon();

        // a format writer flushes its footer on close, so the file must not be removed before that happens
        static::assertLessThan(
            array_search('rm', $filesystem->calls, true),
            array_search('close', $filesystem->calls, true),
        );
    }

    public function test_a_destination_without_an_extension_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Stream path must have an extension');

        (new FilesSink(memory_filesystem(), path('memory://out'), exception_if_exists()))->writeTo();
    }

    public function test_a_pattern_destination_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Destination path can't be pattern");

        (new FilesSink(memory_filesystem(), path('memory://out/*.csv'), exception_if_exists()))->writeTo();
    }

    public function test_a_placeholder_destination_without_partitions_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('contains partition placeholders but rows are not partitioned');

        (new FilesSink(memory_filesystem(), path('memory://out/{name}.csv'), exception_if_exists()))->writeTo();
    }

    public function test_the_same_destination_returns_the_same_stream(): void
    {
        $files = new FilesSink(memory_filesystem(), path('memory://out.csv'), exception_if_exists());

        static::assertSame($files->writeTo(), $files->writeTo());
    }

    public function test_touched_is_answered_per_partition(): void
    {
        $files = new FilesSink(memory_filesystem(), path('memory://out.csv'), exception_if_exists());
        $partition = [new Partition('day', '2024-01-01')];

        static::assertFalse($files->touched($partition));

        $files->writeTo($partition);

        static::assertTrue($files->touched($partition));
        static::assertFalse($files->touched([new Partition('day', '2024-01-02')]));
    }
}
