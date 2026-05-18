<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\ignore;

final class IgnoreModeTest extends FilesystemStreamsTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file(): void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);
        $path = $this->getPath(__FUNCTION__ . '/existing-file.txt');

        $fileStream = $streams->writeTo($path);
        $fileStream->append('different content');
        $streams->closeStreams($path);

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $path = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $fileStream = $streams->writeTo($path);
        $fileStream->append('some content');
        $streams->closeStreams($path);

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }

    protected function streams(): FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(ignore());

        return $streams;
    }
}
