<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Partition;
use Flow\ETL\Partition\NoopFilter;

final class FilesystemStreamsTest extends FilesystemStreamsTestCase
{
    protected function tearDown() : void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_is_open_for_writing() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);

        $streams = $this->streams();
        $streams->writeTo($this->getPath(__FUNCTION__ . '/file.txt'));
        $this->assertTrue($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
        $this->assertCount(1, $streams);
        $streams->closeWriters($this->getPath(__FUNCTION__ . '/file.txt'));
        $this->assertFalse($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
    }

    public function test_read() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'file content',
            ],
        ]);

        $streams = $this->streams();
        $this->assertEquals(
            'file content',
            \file_get_contents($streams->read($this->getPath(__FUNCTION__ . '/file.txt'))->path()->path())
        );
    }

    public function test_read_partitioned() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=a' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        $streams = $this->streams();
        $this->assertEquals(
            'file content',
            \file_get_contents(
                $streams->read($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')])
                    ->path()->path()
            )
        );
    }

    public function test_rm() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'file content',
            ],
        ]);

        $this->assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt')));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'));

        $this->assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/file.txt')->path());
    }

    public function test_rm_partitioned() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=a' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        $this->assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')]));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')]);

        $this->assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/partition=a/file.txt')->path());
    }

    public function test_scan() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file1.txt' => 'file1 content',
                'file2.txt' => 'file2 content',
                'folder' => [
                    'file3.txt' => 'file3 content',
                    'file4.txt' => 'file4 content',
                ],
            ],
        ]);

        $streams = $this->streams();
        $this->assertCount(
            4,
            \iterator_to_array($streams->scan($this->getPath(__FUNCTION__ . '/**/*.txt'), new NoopFilter()))
        );
    }

    protected function streams() : FilesystemStreams
    {
        return new FilesystemStreams($this->fs);
    }
}
