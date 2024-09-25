<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams;

use function Flow\Filesystem\DSL\path_stdout;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path\Filter\KeepAll;

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
        self::assertTrue($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
        self::assertCount(1, $streams);
        $streams->closeStreams($this->getPath(__FUNCTION__ . '/file.txt'));
        self::assertFalse($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
    }

    public function test_open_two_write_streams_to_stdout() : void
    {
        $this->expectExceptionMessage('Only one stdout filesystem stream can be open at the same time');

        $streams = $this->streams();
        $streams->writeTo(path_stdout('json'));
        $streams->writeTo(path_stdout('json'));

    }

    public function test_read() : void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'file content',
            ],
        ]);

        $streams = $this->streams();
        self::assertEquals(
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
        self::assertEquals(
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

        self::assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt')));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'));

        self::assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/file.txt')->path());
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

        self::assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')]));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')]);

        self::assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/partition=a/file.txt')->path());
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
        self::assertCount(
            4,
            \iterator_to_array($streams->list($this->getPath(__FUNCTION__ . '/**/*.txt'), new KeepAll()))
        );
    }

    public function test_write_to_stdout() : void
    {
        $streams = $this->streams();
        $streams->writeTo(path_stdout('json'));

        self::assertCount(1, $streams);
    }

    protected function streams() : FilesystemStreams
    {
        return new FilesystemStreams($this->fstab());
    }
}
