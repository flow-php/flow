<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit;

use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\Context\WrittenFiles;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Tests\Double\FailingCloseFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function file_get_contents;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\Filesystem\DSL\partition;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\stdout_filesystem;
use function iterator_to_array;

final class FileCommitTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public static function save_modes(): Generator
    {
        yield 'overwrite' => [SaveMode::Overwrite];
        yield 'exception if exists' => [SaveMode::ExceptionIfExists];
        yield 'append' => [SaveMode::Append];
        yield 'ignore' => [SaveMode::Ignore];
    }

    #[DataProvider('save_modes')]
    public function test_a_commit_whose_stream_fails_to_close_commits_nothing_and_abort_removes_every_file(SaveMode $saveMode): void
    {
        $this->context->files([__FUNCTION__ => []]);

        $files = new FileCommit(
            new FailingCloseFilesystem($this->fs, healthyStreams: 1),
            $this->context->path(__FUNCTION__ . '/file.txt'),
            $saveMode,
            'r1',
        );
        $files->open([partition('p', 'a')])->append('a');
        $b = $files->open([partition('p', 'b')]);
        $b->append('b');

        try {
            $files->commit([0]);
            static::fail('commit() was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "' . $b->path()->uri() . '" failed', $failure->getMessage());
        }

        $files->abort();

        // every stream closes before the first move, so a close failure commits nothing
        static::assertSame([], WrittenFiles::under($this->context->path(__FUNCTION__)));
    }

    public function test_abort_removes_every_file_even_when_a_stream_fails_to_close(): void
    {
        $this->context->files([__FUNCTION__ => []]);

        $files = new FileCommit(
            new FailingCloseFilesystem($this->fs, failingStreams: 2),
            $this->context->path(__FUNCTION__ . '/file.txt'),
            SaveMode::Overwrite,
            'r1',
        );
        $a = $files->open([partition('p', 'a')]);
        $a->append('a');
        $b = $files->open([partition('p', 'b')]);
        $b->append('b');

        try {
            $files->abort();
            static::fail('abort() was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "' . $a->path()->uri() . '" failed', $failure->getMessage());
        }

        static::assertFileDoesNotExist($a->path()->path());
        static::assertFileDoesNotExist($b->path()->path());
    }

    public function test_abort_leaves_the_destination_untouched(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = new FileCommit($this->fs, $file, SaveMode::Overwrite, 'r1');
        $stream = $files->open();
        $stream->append('half written content');

        $files->abort();

        static::assertSame('existing content', file_get_contents($file->path()));
        static::assertFileDoesNotExist($stream->path()->path());
        static::assertSame([], WrittenFiles::temporaryOutputs($this->cacheDir));
    }

    public function test_abort_leaves_a_destination_it_did_not_create(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = new FileCommit($this->fs, $file, SaveMode::Ignore, 'r1');

        static::assertFalse($files->admit(false));

        $files->abort();

        static::assertFileExists($file->path());
        static::assertSame('existing content', file_get_contents($file->path()));
    }

    public function test_abort_removes_the_file_an_append_created_beside_the_existing_one(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'file.txt' => 'existing content',
            ],
        ]);

        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = new FileCommit($this->fs, $file, SaveMode::Append, 'r1');
        $files->open()->append('half written content');

        $files->abort();

        static::assertCount(
            1,
            $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*'))),
        );
        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('existing content', file_get_contents($file->path()));
    }

    #[DataProvider('save_modes')]
    public function test_abort_removes_every_file_the_write_created(SaveMode $saveMode): void
    {
        $this->context->files([__FUNCTION__ => ['kept.txt' => 'kept']]);

        $files = new FileCommit($this->fs, $this->context->path(__FUNCTION__ . '/file.txt'), $saveMode, 'r1');
        $files->open()->append('half written content');

        $files->abort();

        static::assertSame(['kept.txt'], WrittenFiles::under($this->context->path(__FUNCTION__)));
    }

    public function test_abort_after_a_commit_does_nothing(): void
    {
        $this->context->files([__FUNCTION__ => []]);

        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = new FileCommit($this->fs, $file, SaveMode::Overwrite, 'r1');
        $files->open()->append('some content');
        $files->commit([0]);

        $files->abort();

        static::assertSame('some content', file_get_contents($file->path()));
    }

    public function test_open_two_write_streams_to_stdout(): void
    {
        $this->expectExceptionMessage('Only one stream can be open at the same time for php://stdout');

        // one instance: StdOutFilesystem holds its open targets per instance
        $stdout = stdout_filesystem();

        $this->context->commit(path('stdout://a.stdout'), exception_if_exists(), $stdout)->open();
        $this->context->commit(path('stdout://b.stdout'), exception_if_exists(), $stdout)->open();
    }

    public function test_scan(): void
    {
        $this->context->files([
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
            (new FileListing($this->fs))->list($this->context->path(__FUNCTION__ . '/**/*.txt'), new KeepAll()),
            false,
        ));
    }

    public function test_write_to_stdout(): void
    {
        static::assertTrue(
            $this->context
                ->commit(path('stdout://a.stdout'), exception_if_exists(), stdout_filesystem())
                ->open()
                ->isOpen(),
        );
    }
}
