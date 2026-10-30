<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function file_put_contents;
use function filesize;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
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
                $reader->extract(
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
}
