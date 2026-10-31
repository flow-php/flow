<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function array_map;
use function array_merge;
use function file_put_contents;
use function filesize;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_instance_of;
use function iterator_to_array;
use function mkdir;

final class CSVSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.csv'))->withBatchSize(3);
        $reader = $extractor->reader();

        static::assertSame(ExtractedRows::of($extractor)->toArray(), ReaderRoundTrip::rows($reader, flow_context()));
        static::assertSame(
            ReaderRoundTrip::extractorBatches($extractor, flow_context()),
            ReaderRoundTrip::batches($reader, flow_context()),
        );
        static::assertSame(
            ReaderRoundTrip::rows($reader, flow_context()),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context()),
        );
        static::assertNotSame(
            [],
            DescribeRoundTrip::rebuilt($reader)
                ->describe()
                ?->normalize()['data']['batches']['data']['opener']['data']['dialects'] ?? [],
        );
    }

    public function test_a_placeholder_glob_keeps_its_partition_columns_after_a_rebuild(): void
    {
        $root = ReaderRoundTrip::scratch('csv-placeholders');
        mkdir($root->path() . '/2024');
        mkdir($root->path() . '/2025');
        file_put_contents($root->path() . '/2024/a.csv', "id\n1\n");
        file_put_contents($root->path() . '/2025/b.csv', "id\n2\n");
        $extractor = from_csv($root->suffix('/{year}/*.csv'));

        static::assertSame(
            [['id' => 1, 'year' => '2024'], ['id' => 2, 'year' => '2025']],
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($extractor->reader()), flow_context()),
        );
        static::assertSame(
            ExtractedRows::of($extractor)->toArray(),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($extractor->reader()), flow_context()),
        );

        native_local_filesystem()->rm($root);
    }

    public function test_a_sized_split_is_read_without_listing(): void
    {
        $root = ReaderRoundTrip::scratch('csv-sized');
        file_put_contents($root->path() . '/report.csv', "id,name\n1,a\n2,b\n");
        $file = $root->suffix('/report.csv');
        $counting = new CountingFilesystem(native_local_filesystem());
        $reader = from_csv($root->suffix('/*.csv'), filesystem: $counting)->reader();
        $listedBySchema = $counting->listCalls;

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
            iterator_to_array(
                FlowTestCase::extracted(
                    $reader,
                    flow_context()->backend(),
                    (new Criteria())->withSplit(
                        new FileSplit($file, 0, null, (int) filesize($root->path() . '/report.csv')),
                    ),
                ),
                false,
            )[0]->toArray(),
        );
        static::assertSame($listedBySchema, $counting->listCalls);

        native_local_filesystem()->rm($root);
    }

    public function test_an_inferred_reader_refuses_a_diverging_file_after_a_rebuild(): void
    {
        $reader = from_csv(path_real(__DIR__ . '/../../../Fixtures/columns_diverge_refused/*.csv'))
            ->inferSchema(infer_schema()->unionByName(false))
            ->reader();

        $this->expectException(InferredSchemaException::class);

        ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context());
    }

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(
            from_csv(path('memory://a.csv'), filesystem: memory_filesystem())
                ->withSchema(schema(int_schema('id')))
                ->reader()
                ->describe(),
        );
    }

    public function test_a_reader_routes_its_rejects_after_a_rebuild_through_the_runs_lifecycle(): void
    {
        $root = ReaderRoundTrip::scratch('csv-rejects');
        file_put_contents($root->path() . '/orders.csv', "id\n1\nx\n3\n");
        $reader = from_csv($root->suffix('/orders.csv'))
            ->withSchema(schema(int_schema('id')))
            ->withCorruptedRecords(to_floe($root->suffix('/rejects/part-{part}.floe')))
            ->reader();
        $rebuilt = DescribeRoundTrip::rebuilt($reader);
        $loader = type_instance_of(FileLoader::class)->assert($rebuilt->corruptedRecordsLoader());
        $inTask = $loader->inTask(new Part(0, 1, 'run1', false));
        $task = $rebuilt->withCorruptedRecords($inTask);

        $telemetry = new MemoryTelemetryContext();
        $rows = iterator_to_array(
            (new StepRunner())->steps(
                new PipelineSteps(),
                $task->extract(flow_context()->backend(), new Criteria()),
                $telemetry->flowContext,
                $task,
            ),
            false,
        );
        $loader->commit('run1', 1)?->commit([0]);

        static::assertSame(
            [1, 3],
            array_merge(...array_map(static fn(Rows $batch): array => $batch->reduceToArray('id'), $rows)),
        );
        static::assertSame(
            ['x'],
            ExtractedRows::of(from_floe($root->suffix('/rejects/*.floe')))->reduceToArray('record'),
        );
        static::assertCount(
            1,
            $telemetry->logs->entriesContaining(
                'Routed 1 corrupted records to ' . $root->suffix('/rejects/part-{part}.floe')->uri() . '.',
            ),
        );

        native_local_filesystem()->rm($root);
    }

    public function test_a_reader_whose_reject_loader_does_not_describe_does_not_describe(): void
    {
        $rejects = [];

        static::assertNull(
            from_csv(path_real(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.csv'))
                ->withCorruptedRecords(to_array($rejects))
                ->reader()
                ->describe(),
        );
    }
}
