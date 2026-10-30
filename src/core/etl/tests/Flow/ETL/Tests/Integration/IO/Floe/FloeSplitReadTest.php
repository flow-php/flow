<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Floe;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Double\CodecStub;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function iterator_to_array;

final class FloeSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $root = ReaderRoundTrip::scratch('floe');
        FloeFilesContext::writeSections(native_local_filesystem(), $root->uri() . '/a.floe', 2, 50);
        FloeFilesContext::writeSections(native_local_filesystem(), $root->uri() . '/b.floe', 1, 50);
        $extractor = from_floe($root->suffix('/*.floe'))->withBatchSize(30);
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

        native_local_filesystem()->rm($root);
    }

    public function test_a_sized_split_is_read_without_listing(): void
    {
        $root = ReaderRoundTrip::scratch('floe-split');
        FloeFilesContext::writeSections(native_local_filesystem(), $root->uri() . '/a.floe', 2, 10);
        $file = $root->suffix('/a.floe');
        $size = native_local_filesystem()->status($file)?->size;
        $counting = new CountingFilesystem(native_local_filesystem());
        $reader = from_floe($file, filesystem: $counting)->reader();

        $listedBySchema = $counting->listCalls;

        static::assertNotNull($size);
        static::assertSame(
            20,
            array_sum(array_map(
                static fn($rows): int => $rows->count(),
                iterator_to_array(
                    $reader->extract(
                        flow_context()->backend(),
                        (new Criteria())->withSplit(new FileSplit($file, 0, null, $size)),
                    ),
                    false,
                ),
            )),
        );
        static::assertSame($listedBySchema, $counting->listCalls);

        native_local_filesystem()->rm($root);
    }

    public function test_an_unsized_split_lists_its_path(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://a.floe', 1, 4);
        $counting = new CountingFilesystem($memory);
        $reader = from_floe(path('memory://a.floe'), filesystem: $counting)->reader();
        $listedBySchema = $counting->listCalls;

        iterator_to_array($reader->extract(
            flow_context()->backend(),
            (new Criteria())->withSplit(new FileSplit(path('memory://a.floe'), 0, null)),
        ));

        static::assertSame($listedBySchema + 1, $counting->listCalls);
    }

    public function test_a_codec_that_is_not_noop_does_not_describe(): void
    {
        $root = ReaderRoundTrip::scratch('floe-codec');
        FloeFilesContext::writeSections(native_local_filesystem(), $root->uri() . '/a.floe', 1, 1);

        static::assertNull(
            from_floe($root->suffix('/a.floe'), new CodecStub(7))
                ->withSchema(schema(int_schema('id')))
                ->reader()
                ->describe(),
        );
        static::assertNotNull(from_floe($root->suffix('/a.floe'))->reader()->describe());

        native_local_filesystem()->rm($root);
    }

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://a.floe', 1, 1);

        static::assertNull(from_floe(path('memory://a.floe'), filesystem: $memory)->reader()->describe());
    }

    public function test_the_limit_travels_with_the_split(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://a.floe', 1, 10);

        static::assertSame(
            3,
            iterator_to_array(
                from_floe(path('memory://a.floe'), filesystem: $memory)
                    ->reader()
                    ->extract(
                        flow_context()->backend(),
                        (new Criteria(3))->withSplit(new FileSplit(path('memory://a.floe'), 0, null)),
                    ),
                false,
            )[0]->count(),
        );
    }
}
