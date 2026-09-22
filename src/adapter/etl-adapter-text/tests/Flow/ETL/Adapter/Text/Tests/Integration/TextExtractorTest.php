<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\Double\UnsizedFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class TextExtractorTest extends FlowTestCase
{
    public function test_extracting_text_file(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = data_frame()->read(from_text($path))->fetch();

        static::assertEquals(type_string(), $rows->schema()->get('text')->type());
        static::assertSame(1024, $rows->count());
    }

    public function test_limit(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->withBatchSize(1);

        self::assertExtractedRowsCount(2, $extractor, flow_context(config()), limit: 2);
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['text' => 'line a', 'date' => '2026-01-01'],
                ['text' => 'line b', 'date' => null],
            ],
            data_frame()
                ->read(from_text(__DIR__ . '/../Fixtures/cross_stream/*/data.txt'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('text'), str_schema('_input_file_uri')),
            from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->withMetadataColumns(true)->schema(),
        );
    }

    public function test_schema_describes_a_single_text_column(): void
    {
        static::assertEquals(schema(str_schema('text')), from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->schema());
    }

    public function test_signal_stop(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_signal_stop_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $generator = from_text(__DIR__ . '/../Fixtures/cross_stream/*/data.txt')
            ->withBatchSize(10)
            ->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_limit_reached_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $extractor = from_text(__DIR__ . '/../Fixtures/cross_stream/*/data.txt')->withBatchSize(10);

        static::assertCount(1, ExtractedRows::of($extractor, limit: 1));
    }

    public function test_metadata_columns_extend_a_declared_schema(): void
    {
        static::assertEquals(
            schema(str_schema('line'), str_schema('_input_file_uri')),
            from_text(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv')
                ->withSchema(schema(str_schema('line')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_text(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'))->isRepeatable());
    }

    public function test_a_limited_read_closes_its_stream(): void
    {
        $filesystem = new RecordingFilesystem(native_local_filesystem());
        $extractor = (new TextExtractor(
            path_real(__DIR__ . '/../Fixtures/parity_lines.txt'),
            $filesystem,
        ))->withBatchSize(1);

        iterator_to_array($extractor->extract(flow_context(config()), limit: 1));

        static::assertContains('closeSource', $filesystem->calls);
    }

    public function test_a_stopped_read_closes_its_stream(): void
    {
        $filesystem = new RecordingFilesystem(native_local_filesystem());
        $generator = (new TextExtractor(path_real(__DIR__ . '/../Fixtures/parity_lines.txt'), $filesystem))
            ->withBatchSize(1)
            ->extract(flow_context(config()));

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertContains('closeSource', $filesystem->calls);
    }

    public function test_it_declares_the_listed_byte_total_exactly(): void
    {
        $statistics = from_text(
            path('memory://dir/*.txt'),
            MemoryFiles::with(['memory://dir/a.txt' => 'abcdefghij', 'memory://dir/b.txt' => 'abcde']),
        )->statistics();

        static::assertEquals(Cardinality::exact(15), $statistics->size);
        static::assertEquals(Cardinality::unknown(), $statistics->rows);
    }

    public function test_a_member_without_a_size_makes_the_size_unknown(): void
    {
        $statistics = from_text(
            path('memory://dir/*.txt'),
            new UnsizedFilesystem(MemoryFiles::with([
                'memory://dir/a.txt' => 'abcdefghij',
                'memory://dir/b.txt' => 'abcde',
            ]), ['memory://dir/b.txt']),
        )->statistics();

        static::assertEquals(Cardinality::unknown(), $statistics->size);
    }

    public function test_the_listing_is_read_once(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with([
            'memory://dir/a.txt' => 'abcdefghij',
            'memory://dir/b.txt' => 'abcde',
        ]));
        $extractor = from_text(path('memory://dir/*.txt'), $filesystem);

        $extractor->statistics();
        $extractor->statistics();

        static::assertSame(1, $filesystem->listCalls);
    }
}
