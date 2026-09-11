<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink;

use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Path\Filter\KeepAll;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\stdout_filesystem;
use function iterator_to_array;

final class FilesSinkTest extends FilesSinkTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_abandon_leaves_the_destination_untouched(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::Overwrite);
        $stream = $files->writeTo();
        $stream->append('half written content');

        $files->abandon();

        static::assertSame('existing content', file_get_contents($file->path()));
        // list('/*') cannot see a dot-prefixed tmp file, so the leftover is asserted by its own path
        static::assertFileDoesNotExist($stream->path()->path());
    }

    public function test_abandon_leaves_a_destination_it_did_not_create(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::Ignore);
        $files->writeTo()->append('ignored content');

        $files->abandon();

        static::assertFileExists($file->path());
        static::assertSame('existing content', file_get_contents($file->path()));
    }

    public function test_abandon_removes_the_file_it_created_under_append(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::Append);
        $files->writeTo()->append('half written content');

        $files->abandon();

        static::assertCount(
            1,
            $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*'))),
        );
        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('existing content', file_get_contents($file->path()));
    }

    public function test_abandon_removes_the_file_it_created_under_exception_if_exists(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::ExceptionIfExists);
        $files->writeTo()->append('half written content');

        $files->abandon();

        static::assertFileDoesNotExist($file->path());
    }

    public function test_abandon_removes_the_temporary_file(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::Overwrite);
        $stream = $files->writeTo();
        $stream->append('half written content');

        static::assertFileExists($stream->path()->path());

        $files->abandon();

        static::assertFileDoesNotExist($stream->path()->path());
        static::assertFileDoesNotExist($file->path());
    }

    public function test_abandoning_a_published_set_does_nothing(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = new FilesSink($this->fs, $file, SaveMode::Overwrite);
        $files->writeTo()->append('some content');
        $files->publish();

        $files->abandon();

        static::assertSame('some content', file_get_contents($file->path()));
    }

    public function test_touched_reports_a_file_this_run_has_written_to(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = $this->files($file);

        static::assertFalse($files->touched());

        $files->writeTo();

        static::assertTrue($files->touched());

        $files->publish();

        static::assertFalse($files->touched());
    }

    public function test_open_streams_lists_only_streams_that_are_still_open(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = $this->files($file);
        $stream = $files->writeTo();

        static::assertSame([$stream], iterator_to_array($files->openStreams(), false));

        $stream->close();

        static::assertSame([], iterator_to_array($files->openStreams(), false));
    }

    public function test_open_two_write_streams_to_stdout(): void
    {
        $this->expectExceptionMessage('Only one stream can be open at the same time for php://stdout');

        // one instance: StdOutFilesystem holds its open targets per instance
        $stdout = stdout_filesystem();

        $this->files(path('stdout://a.stdout'), $stdout)->writeTo();
        $this->files(path('stdout://b.stdout'), $stdout)->writeTo();
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

        static::assertCount(4, iterator_to_array(
            (new FileListing($this->fs))->list($this->getPath(__FUNCTION__ . '/**/*.txt'), new KeepAll()),
            false,
        ));
    }

    public function test_write_to_stdout(): void
    {
        $files = $this->files(path('stdout://a.stdout'), stdout_filesystem());
        $files->writeTo();

        static::assertTrue($files->touched());
    }

    protected function saveMode(): SaveMode
    {
        return exception_if_exists();
    }
}
