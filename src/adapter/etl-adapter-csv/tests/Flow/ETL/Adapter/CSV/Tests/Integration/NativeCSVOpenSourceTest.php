<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\NativeCSVOpenSource;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingSourceStream;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\ETL\Schema\Inference\TypeFloor;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function class_exists;
use function file_get_contents;
use function Flow\ETL\DSL\infer_schema;
use function Flow\Filesystem\DSL\path;
use function implode;
use function iterator_to_array;

final class NativeCSVOpenSourceTest extends FlowTestCase
{
    public function test_close_closes_the_stream_once(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = CSVFixtureContext::openNative('two_rows.csv', $counting);

        $open->close();

        static::assertSame(1, $counting->closedStreams());
    }

    public function test_columns_of_an_empty_source_is_empty(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('empty.csv');

        try {
            static::assertSame([], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reads_a_quoted_multiline_header_as_one_record(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('multiline_header.csv');

        try {
            static::assertSame(['id', "na\nme"], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reports_the_resolved_header(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('header_only.csv');

        try {
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_without_a_header_line_are_generated(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('two_columns.csv', options: new CSVReadOptions(withHeader: false));

        try {
            static::assertSame(['e00', 'e01'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_every_row_with_the_full_key_set(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('ragged.csv');

        try {
            $records = iterator_to_array($open->records(), false);

            static::assertCount(3, $records);

            foreach ($records as $record) {
                static::assertSame(['id', 'name', 'v'], array_keys($record->values));
            }
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_one_logical_record_per_iteration(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('multiline_strings.csv');

        try {
            $records = iterator_to_array($open->records(), false);

            static::assertNotSame([], $records);
            static::assertStringContainsString("\n", implode('', array_map('strval', $records[0]->values)));
        } finally {
            $open->close();
        }
    }

    #[DataProviderExternal(CSVFixtureContext::class, 'fixtures')]
    public function test_native_and_php_open_sources_yield_identical_records(string $fixture): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $php = CSVFixtureContext::openPhp($fixture);
        $native = CSVFixtureContext::openNative($fixture);

        try {
            static::assertSame(CSVFixtureContext::records($php), CSVFixtureContext::records($native));
        } finally {
            $php->close();
            $native->close();
        }
    }

    /**
     * @param positive-int $chunkSize
     */
    #[TestWith([1])]
    #[TestWith([7])]
    #[TestWith([4096])]
    public function test_records_are_identical_across_chunk_sizes(int $chunkSize): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $contents = (string) file_get_contents(CSVFixtureContext::path('multiline_strings.csv'));

        static::assertSame(
            CSVFixtureContext::records(CSVFixtureContext::openNativeStream(
                new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
            )),
            CSVFixtureContext::records(CSVFixtureContext::openNativeStream(
                new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
                (new CSVReadOptions())->withCharactersReadInLine($chunkSize),
            )),
        );
    }

    public function test_a_remote_stream_is_read_in_steps_of_characters_read_in_line(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $stream = new LengthCapturingSourceStream("id,name\n1,a\n", path('s3://bucket/a.csv'));

        CSVFixtureContext::records(CSVFixtureContext::openNativeStream(
            $stream,
            (new CSVReadOptions())->withCharactersReadInLine(4096),
        ));

        static::assertSame([4096], $stream->capturedIterateLengths);
    }

    public function test_a_remote_stream_without_characters_read_in_line_is_read_in_default_chunks(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $stream = new LengthCapturingSourceStream("id,name\n1,a\n", path('s3://bucket/a.csv'));

        CSVFixtureContext::records(CSVFixtureContext::openNativeStream($stream));

        static::assertSame([NativeCSVOpenSource::CHUNK], $stream->capturedIterateLengths);
    }

    #[TestWith(['multiline_header.csv'])]
    #[TestWith(['with_utf8_bom.csv'])]
    #[TestWith(['semicolon.csv'])]
    #[TestWith(['single_quotes_csv.csv'])]
    public function test_columns_resolves_the_same_header_on_both_paths(string $fixture): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $php = CSVFixtureContext::openPhp($fixture);
        $native = CSVFixtureContext::openNative($fixture);

        try {
            static::assertSame($php->columns(), $native->columns());
        } finally {
            $php->close();
            $native->close();
        }
    }

    public function test_a_zero_byte_source_resolves_no_header_on_both_paths(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $php = CSVFixtureContext::openPhp('empty.csv');
        $native = CSVFixtureContext::openNative('empty.csv');

        try {
            static::assertSame([], $php->columns());
            static::assertSame([], $native->columns());
        } finally {
            $php->close();
            $native->close();
        }
    }

    public function test_is_supported_is_false_without_the_extension(): void
    {
        if (class_exists(RustCSVReaderNative::class, false)) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is loaded');
        }

        static::assertFalse(NativeCSVOpenSource::isSupported());
    }

    /**
     * @param int<0, max>|-1 $rowBudget
     */
    #[TestWith([2, 2])]
    #[TestWith([-1, 5])]
    #[TestWith([10, 5])]
    public function test_sniff_folds_at_most_the_row_budget(int $rowBudget, int $rows): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('five_rows.csv');

        try {
            static::assertSame(
                $rows,
                $open->sniff(
                    ['id', 'name'],
                    $rowBudget,
                    infer_schema()->build(),
                    new StringTypeNarrower(InferredTypes::default()->toArray()),
                )->rows(),
            );
        } finally {
            $open->close();
        }
    }

    public function test_sniff_folds_date_and_time_zone_columns_like_observing_the_records(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $contents = "at,day,zone,n\n2024-01-01 10:00:00,2024-01-01,Europe/Warsaw,\n2024-02-02 11:00:00,2024-02-02,+02:00,1\n";
        $inference = infer_schema()->build();
        $typer = new StringTypeNarrower($inference->candidates()->toArray());
        $names = ['at', 'day', 'zone', 'n'];
        $observed = (new SchemaInferrer($inference, $typer))->sniff(
            $names,
            CSVFixtureContext::openNativeStream(
                new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
            )->records(),
            -1,
        );
        $sniffed = CSVFixtureContext::openNativeStream(
            new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
        )->sniff($names, -1, $inference, $typer);

        static::assertEquals(
            $observed->schema(new TypeFloor($inference->candidates())),
            $sniffed->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_columns_of_a_header_without_a_trailing_newline(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        static::assertSame(
            ['id', 'name'],
            CSVFixtureContext::openNativeStream(
                new LengthCapturingSourceStream('id,name', path('s3://bucket/a.csv')),
            )->columns(),
        );
    }
}
