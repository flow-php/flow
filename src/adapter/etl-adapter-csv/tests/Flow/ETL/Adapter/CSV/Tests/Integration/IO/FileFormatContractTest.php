<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO;

use Flow\ETL\Adapter\CSV\Tests\Context\CSVFileFormat;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\ETL\Tests\Context\FileFormatContract;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_column;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;
use function substr_count;

/**
 * etl's file format contract, run over this package's format.
 */
final class FileFormatContractTest extends FlowIntegrationTestCase
{
    /**
     * @return Generator<string, array{FileFormat}>
     */
    public static function formats(): Generator
    {
        yield 'csv' => [new CSVFileFormat()];
    }

    #[DataProvider('formats')]
    public function test_overwrite_replaces_what_the_destination_held(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->overwriteReplacesWhatTheDestinationHeld($format);
    }

    #[DataProvider('formats')]
    public function test_append_keeps_what_the_destination_held(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->appendKeepsWhatTheDestinationHeld($format);
    }

    #[DataProvider('formats')]
    public function test_ignore_leaves_an_existing_destination_untouched(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->ignoreLeavesAnExistingDestinationUntouched($format);
    }

    #[DataProvider('formats')]
    public function test_exception_if_exists_refuses_an_existing_destination(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->exceptionIfExistsRefusesAnExistingDestination($format);
    }

    #[DataProvider('formats')]
    public function test_a_batch_of_zero_rows_creates_no_file(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aBatchOfZeroRowsCreatesNoFile($format);
    }

    #[DataProvider('formats')]
    public function test_a_partitioned_write_reads_back_every_partition(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aPartitionedWriteReadsBackEveryPartition($format);
    }

    #[DataProvider('formats')]
    public function test_a_loader_in_a_task_survives_describe(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aLoaderInATaskSurvivesDescribe($format);
    }

    #[DataProvider('formats')]
    public function test_an_extractor_reader_survives_describe(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->anExtractorReaderSurvivesDescribe($format);
    }

    #[DataProvider('formats')]
    public function test_the_splits_read_every_row_once(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->theSplitsReadEveryRowOnce($format);
    }

    #[DataProvider('formats')]
    public function test_metadata_columns_name_the_source_file(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->metadataColumnsNameTheSourceFile($format);
    }

    #[DataProvider('formats')]
    public function test_the_batch_size_bounds_every_batch(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->theBatchSizeBoundsEveryBatch($format);
    }

    #[DataProvider('formats')]
    public function test_refresh_lists_the_files_written_after_a_read(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->refreshListsTheFilesWrittenAfterARead($format);
    }

    #[DataProvider('formats')]
    public function test_a_failed_run_leaves_no_file_at_the_destination(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aFailedRunLeavesNoFileAtTheDestination($format);
    }

    #[DataProvider('formats')]
    public function test_an_extractor_rejects_a_path_its_filesystem_does_not_serve(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->anExtractorRejectsAPathItsFilesystemDoesNotServe($format);
    }

    #[DataProvider('formats')]
    public function test_a_loader_rejects_a_path_its_filesystem_does_not_serve(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aLoaderRejectsAPathItsFilesystemDoesNotServe($format);
    }

    #[DataProvider('formats')]
    public function test_reads_back_through_the_same_filesystem_instance(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->readsBackThroughTheSameFilesystemInstance($format);
    }

    #[DataProvider('formats')]
    public function test_a_pushed_filter_returns_what_the_filter_node_returns(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aPushedFilterReturnsWhatTheFilterNodeReturns(
            $format,
            ref('id')->notEquals(lit('b')),
        );
    }

    public function test_csv_header_is_written_exactly_once_across_batches(): void
    {
        $destination = $this->cacheDir->suffix('header-once.csv');

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(1)
            ->write(to_csv($destination))
            ->run();

        static::assertSame(1, substr_count($this->fs()->readFrom($destination)->content(), 'id'));
    }

    public function test_reading_a_remote_path_without_a_filesystem_throws_naming_the_argument(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.csv". Pass the filesystem that handles this scheme, '
            . 'e.g. from_csv($path, filesystem: aws_s3_filesystem(...)).',
        );

        from_csv(path('aws-s3://bucket/orders.csv'));
    }

    public function test_writing_a_remote_path_without_a_filesystem_throws_naming_the_argument(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "aws-s3://bucket/orders.csv". Pass the filesystem that handles this scheme, '
            . 'e.g. to_csv($path, filesystem: aws_s3_filesystem(...)).',
        );

        to_csv(path('aws-s3://bucket/orders.csv'));
    }

    public function test_reads_through_the_given_filesystem(): void
    {
        $counting = new CountingFilesystem($this->fs);
        $uri = $this->cacheDir->path() . '/orders.csv';

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_csv(path($uri)))
            ->run();

        $rows = [];
        df()
            ->read(from_csv(path($uri), filesystem: $counting))
            ->write(to_array($rows))
            ->run();

        static::assertSame([1, 2], array_column($rows, 'id'));
        static::assertSame(
            3,
            $counting->readFromCalls,
            'header() + the sample pass + the read (which checks the divergence on its own stream), all through the filesystem it was given',
        );
    }
}
