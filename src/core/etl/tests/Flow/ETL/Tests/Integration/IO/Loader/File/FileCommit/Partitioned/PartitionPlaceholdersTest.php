<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\Partitioned;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function file_get_contents;
use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class PartitionPlaceholdersTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public function test_append_mode_randomizes_existing_placeholder_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                '123456-PL.csv' => 'existing content',
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $files = new FileCommit($this->fs, $file, append(), 'r1');
        $files->open([new Partition('order-name', '123456-PL')])->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($this->context->path(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(2, $files);

        foreach ($files as $streamFile) {
            static::assertStringStartsWith('123456-PL', $streamFile->path->basename());
            static::assertStringEndsWith('.csv', $streamFile->path->basename());
        }
    }

    public function test_list_attaches_partitions_from_placeholders(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'order-year=2024' => [
                    '123456-PL.csv' => 'file content',
                ],
            ],
        ]);

        $listed = iterator_to_array(
            (new FileListing($this->fs))->list(
                $this->context->path(__FUNCTION__ . '/order-year=*/{order-name}.csv'),
                new OnlyFiles(),
            ),
            false,
        );

        static::assertCount(1, $listed);

        $partitions = $listed[0]->path->partitions();

        static::assertCount(2, $partitions);
        static::assertSame('2024', $partitions->get('order-year')->value);
        static::assertSame('123456-PL', $partitions->get('order-name')->value);
    }

    public function test_overwrite_mode_sweeps_appended_siblings_of_a_placeholder_file(): void
    {
        // an earlier Append run leaves 123456-PL-<run>.csv next to 123456-PL.csv; Overwrite must clear both
        $this->context->files([
            __FUNCTION__ => [
                '123456-PL.csv' => 'existing content',
                '123456-PL-0123456789abcdef.csv' => 'appended content',
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $files = new FileCommit($this->fs, $file, overwrite(), 'r1');
        $files->open([new Partition('order-name', '123456-PL')])->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($this->context->path(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertSame('new content', file_get_contents($files[0]->path->path()));
    }

    public function test_overwrite_mode_replaces_existing_placeholder_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                '123456-PL.csv' => 'existing content',
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $files = new FileCommit($this->fs, $file, overwrite(), 'r1');
        $files->open([new Partition('order-name', '123456-PL')])->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($this->context->path(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertSame('new content', file_get_contents($files[0]->path->path()));
    }

    public function test_write_to_placeholder_path_consuming_all_partitions(): void
    {
        $this->context->files([__FUNCTION__ => []]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $files = $this->context->commit($file, exception_if_exists());
        $files->open([new Partition('order-name', '123456-PL')])->append('file content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($this->context->path(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }

    public function test_write_to_placeholder_path_with_remaining_partitions(): void
    {
        $this->context->files([__FUNCTION__ => []]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $files = $this->context->commit($file, exception_if_exists());
        $files->open([
            new Partition('order-year', '2024'),
            new Partition('order-name', '123456-PL'),
        ])->append('file content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($this->context->path(__FUNCTION__)->path() . '/**/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertStringContainsString('order-year=2024', $files[0]->path->path());
    }

    public function test_write_to_placeholder_path_without_partitions(): void
    {
        $this->context->files([__FUNCTION__ => []]);
        $file = $this->context->path(__FUNCTION__ . '/{order-name}.csv');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('contains partition placeholders but rows are not partitioned');

        $this->context->commit($file, exception_if_exists())->open();
    }
}
