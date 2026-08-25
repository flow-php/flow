<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink\NotPartitioned;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\Integration\Filesystem\FilesSink\FilesSinkTestCase;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\exception_if_exists;

final class ExceptionIfExistsModeTest extends FilesSinkTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file(): void
    {
        $this->expectExceptionMessageMatches(
            '/Destination path (.*) already exists, please change path to different or set different SaveMode/',
        );

        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);

        $this->files($this->getPath(__FUNCTION__ . '/existing-file.txt'))->writeTo();
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $files = $this->files($file);
        $files->writeTo()->append('some content');
        $files->publish();

        static::assertFileExists($file->path());
        static::assertSame('some content', file_get_contents($file->path()));
    }

    protected function saveMode(): SaveMode
    {
        return exception_if_exists();
    }
}
