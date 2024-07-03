<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\overwrite;
use Flow\ETL\Filesystem\{FilesystemStreams};
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Flow\Filesystem\Path;

final class OverwriteModeTest extends FilesystemStreamsTestCase
{
    protected function tearDown() : void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);

        $fileStream = $streams->writeTo($path = $this->getPath(__FUNCTION__ . '/existing-file.txt'));
        self::assertStringEndsWith(FilesystemStreams::FLOW_TMP_SUFFIX, $fileStream->path()->path());
        $fileStream->append('some other content');
        self::assertSame('some content', \file_get_contents($path->path()));

        $streams->closeWriters($path);

        self::assertSame('some other content', \file_get_contents($path->path()));

        self::assertCount(1, $files = \iterator_to_array($this->fs()->list(new Path($path->parentDirectory()->path() . '/*'))));
        self::assertSame('existing-file.txt', $files[0]->path->basename());
    }

    public function test_open_stream_for_non_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([__FUNCTION__ => []]);
        $path = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $fileStream = $streams->writeTo($path);
        $fileStream->append('some content');
        $streams->closeWriters($path);

        self::assertFileExists($path->path());
        self::assertSame('some content', \file_get_contents($path->path()));
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setSaveMode(overwrite());

        return $streams;
    }
}
