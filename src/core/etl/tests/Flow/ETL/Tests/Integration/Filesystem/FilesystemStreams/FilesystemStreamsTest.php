<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path\Filter\KeepAll;
use Override;

use function file_get_contents;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FilesystemStreamsTest extends FilesystemStreamsTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_is_open_for_writing(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);

        $streams = $this->streams();
        $streams->writeTo($this->getPath(__FUNCTION__ . '/file.txt'));
        static::assertTrue($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
        static::assertCount(1, $streams);
        $streams->closeStreams($this->getPath(__FUNCTION__ . '/file.txt'));
        static::assertFalse($streams->isOpen($this->getPath(__FUNCTION__ . '/file.txt')));
    }

    public function test_open_two_write_streams_to_stdout(): void
    {
        $this->expectExceptionMessage('Only one stream can be open at the same time for php://stdout');

        $streams = $this->streams();
        $streams->writeTo(path('stdout://a.stdout'));
        $streams->writeTo(path('stdout://b.stdout'));
    }

    public function test_read(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'file content',
            ],
        ]);

        $streams = $this->streams();
        static::assertEquals(
            'file content',
            file_get_contents(
                $streams
                    ->read($this->getPath(__FUNCTION__ . '/file.txt'))
                    ->path()
                    ->path(),
            ),
        );
    }

    public function test_read_partitioned(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=a' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        $streams = $this->streams();
        static::assertEquals(
            'file content',
            file_get_contents(
                $streams
                    ->read($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')])
                    ->path()
                    ->path(),
            ),
        );
    }

    public function test_rm(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'file content',
            ],
        ]);

        static::assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt')));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'));

        static::assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/file.txt')->path());
    }

    public function test_rm_partitioned(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=a' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        static::assertTrue($this->streams()->exists($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition(
            'partition',
            'a',
        )]));
        $this->streams()->rm($this->getPath(__FUNCTION__ . '/file.txt'), [new Partition('partition', 'a')]);

        static::assertFileDoesNotExist($this->getPath(__FUNCTION__ . '/partition=a/file.txt')->path());
    }

    public function test_scan(): void
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
        static::assertCount(
            4,
            iterator_to_array($streams->list($this->getPath(__FUNCTION__ . '/**/*.txt'), new KeepAll())),
        );
    }

    public function test_write_to_stdout(): void
    {
        $streams = $this->streams();
        $streams->writeTo(path('stdout://a.stdout'));

        static::assertCount(1, $streams);
    }

    protected function streams(): FilesystemStreams
    {
        return new FilesystemStreams($this->fstab());
    }
}
