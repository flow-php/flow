<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\overwrite;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;

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
        $this->assertStringEndsWith(FilesystemStreams::FLOW_TMP_SUFFIX, $fileStream->path()->path());
        \fwrite($fileStream->resource(), 'some other content');
        $this->assertSame('some content', \file_get_contents($path->path()));

        $streams->closeWriters($path);

        $this->assertSame('some other content', \file_get_contents($path->path()));

        $this->assertCount(1, $files = \iterator_to_array($this->fs->scan(new Path($path->parentDirectory()->path() . '/*'))));
        $this->assertSame('existing-file.txt', $files[0]->basename());
    }

    public function test_open_stream_for_non_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([__FUNCTION__ => []]);
        $path = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $fileStream = $streams->writeTo($path);
        \fwrite($fileStream->resource(), 'some content');
        $streams->closeWriters($path);

        $this->assertFileExists($path->path());
        $this->assertSame('some content', \file_get_contents($path->path()));
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fs);
        $streams->setSaveMode(overwrite());

        return $streams;
    }
}
