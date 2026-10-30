<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\IO;

use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFileFormat;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\ETL\Tests\Context\FileFormatContract;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_column;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;

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
        yield 'xlsx' => [new ExcelFileFormat()];
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
    public function test_a_pushed_filter_returns_what_the_filter_node_returns(FileFormat $format): void
    {
        (new FileFormatContract($this->cacheDir))->aPushedFilterReturnsWhatTheFilterNodeReturns(
            $format,
            ref('id')->notEquals(lit('b')),
        );
    }

    /**
     * OpenSpout drives ZipArchive, which takes a local path and cannot read a memory:// stream, so the read itself never
     * crosses the Filesystem. The one call that does is the format sniff, and only an extension-less path needs it - a
     * *.xlsx path is typed by its extension and opens nothing.
     */
    public function test_an_extension_less_path_is_sniffed_through_the_given_filesystem(): void
    {
        $written = path($this->cacheDir->path() . '/family-excel.xlsx');
        $sniffed = path($this->cacheDir->path() . '/family-excel');
        $counting = new CountingFilesystem($this->fs);

        df()
            ->read(from_array([['id' => 'a']]))
            ->write(to_excel($written))
            ->run();

        $this->fs->mv($written, $sniffed);

        $rows = [];
        df()->read(from_excel($sniffed, filesystem: $counting))->write(to_array($rows))->run();

        static::assertSame(['a'], array_column($rows, 'id'));
        // once: the undeclared read sniffs the format for the schema sample, and the read loop reads on from the
        // sample's open sheet
        static::assertSame(1, $counting->readFromCalls);
    }
}
