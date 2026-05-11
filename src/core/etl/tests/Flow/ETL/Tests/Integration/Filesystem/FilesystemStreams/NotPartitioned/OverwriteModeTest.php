<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;

use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;

final class OverwriteModeTest extends FilesystemStreamsTestCase
{
    #[\Override]
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

        $fileStream = $streams->writeTo($path = $this->getPath(__FUNCTION__ . '/existing-file.txt'));
        static::assertStringContainsString(FilesystemStreams::FLOW_TMP_FILE_PREFIX, $fileStream->path()->path());
        $fileStream->append('some other content');
        static::assertSame('some content', \file_get_contents($path->path()));

        $streams->closeStreams($path);

        static::assertSame('some other content', \file_get_contents($path->path()));

        static::assertCount(
            1,
            $files = \iterator_to_array($this->fs()->list(path($path->parentDirectory()->path() . '/*'))),
        );
        static::assertSame('existing-file.txt', $files[0]->path->basename());
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $streams = $this->streams();
        $this->setupFiles([__FUNCTION__ => []]);
        $path = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $fileStream = $streams->writeTo($path);
        $fileStream->append('some content');
        $streams->closeStreams($path);

        static::assertFileExists($path->path());
        static::assertSame('some content', \file_get_contents($path->path()));
    }

    protected function streams(): FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(overwrite());

        return $streams;
    }
}
