<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\NativeCSVOpenSource;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingSourceStream;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\ETL\Schema\Inference\TypeFloor;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function class_exists;
use function count;
use function file_get_contents;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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

    public function test_headers_are_the_header_records_resolved(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('header_only.csv');

        try {
            static::assertSame([], $open->headers());
            static::assertSame([], iterator_to_array($open->records(), false));
            static::assertSame(['id', 'name'], $open->headers());
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
                static::assertSame(['id', 'name', 'v'], array_keys($record));
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
            static::assertStringContainsString("\n", implode('', array_map('strval', $records[0])));
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

    public function test_produced_rows_and_bytes_exclude_the_header(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = CSVFixtureContext::openNative('five_rows.csv');

        try {
            iterator_to_array($open->records(), false);

            static::assertSame(5, $open->producedRows());
            static::assertSame(20, $open->producedBytes());
        } finally {
            $open->close();
        }
    }

    /**
     * Samples that stop before EOF: SourceStream::readLines() hides whether the last line ended with "\n", so the PHP
     * path counts a last record without one a byte longer than the native path does (CSVLineReader::lastRecordBytes()).
     */
    #[TestWith(['annual-enterprise-survey-2019-financial-year-provisional-csv.csv'])]
    #[TestWith(['orders_flow.csv'])]
    #[TestWith(['five_rows.csv'])]
    #[TestWith(['fold_traps.csv'])]
    #[TestWith(['ragged.csv'])]
    public function test_php_and_native_paths_agree_on_bytes_per_row(string $fixture): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $header = CSVFixtureContext::openPhp($fixture);
        $names = $header->columns();
        $header->close();
        $php = CSVFixtureContext::openPhp($fixture);
        $native = CSVFixtureContext::openNative($fixture);
        $typer = new StringTypeNarrower(infer_schema()->build()->candidates()->toArray());

        try {
            $php->sniff($names, 2, new SchemaInference(), $typer);
            $native->sniff($names, 2, new SchemaInference(), $typer);

            static::assertSame($php->producedRows(), $native->producedRows());
            static::assertSame($php->producedBytes(), $native->producedBytes());
        } finally {
            $php->close();
            $native->close();
        }
    }

    public function test_batches_are_batch_size_rows(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n")),
            schema(int_schema('id'), str_schema('name')),
            2,
        );

        static::assertIsArray($batches);
        static::assertSame([2, 2, 1], array_map(count(...), $batches));
    }

    public function test_batches_skip_undeclared_header_columns(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        static::assertSame(
            [[['id' => 1, 'name' => 'a']]],
            CSVFixtureContext::batches(
                CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id,name,extra\n1,a,x\n")),
                schema(int_schema('id'), str_schema('name')),
                10,
            ),
        );
    }

    public function test_batches_pad_a_missing_nullable_column(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        static::assertSame(
            [[['id' => 1, 'name' => null]]],
            CSVFixtureContext::batches(
                CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n1\n")),
                schema(int_schema('id'), str_schema('name', nullable: true)),
                10,
            ),
        );
    }

    public function test_batches_refuse_a_missing_not_null_column(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $refusal = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n1\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertInstanceOf(SchemaMismatchException::class, $refusal);
        static::assertSame(
            (new SchemaMismatchException(0, ColumnMismatchException::missingColumn(str_schema('name'))))->getMessage(),
            $refusal->getMessage(),
        );
        static::assertSame(0, $refusal->rowIndex);
    }

    public function test_batches_refused_value_wins_over_absence(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $refusal = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n1\nx\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertInstanceOf(SchemaMismatchException::class, $refusal);
        static::assertSame(
            (new SchemaMismatchException(1, ColumnMismatchException::valueDoesNotMatch(
                int_schema('id'),
                'x',
            )))->getMessage(),
            $refusal->getMessage(),
        );
        static::assertSame(1, $refusal->rowIndex);
    }

    public function test_batches_refusal_tie_goes_to_the_first_definition(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $refusal = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("a,b\nx,y\n")),
            schema(int_schema('b'), int_schema('a')),
            10,
        );

        static::assertInstanceOf(SchemaMismatchException::class, $refusal);
        static::assertSame(
            (new SchemaMismatchException(0, ColumnMismatchException::valueDoesNotMatch(
                int_schema('b'),
                'y',
            )))->getMessage(),
            $refusal->getMessage(),
        );
        static::assertSame(0, $refusal->rowIndex);
    }

    public function test_batches_refuse_an_empty_to_null_null_under_not_null(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $refusal = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id,name\n1,a\n2,\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertInstanceOf(SchemaMismatchException::class, $refusal);
        static::assertSame(
            (new SchemaMismatchException(1, ColumnMismatchException::valueDoesNotMatch(
                str_schema('name'),
                null,
            )))->getMessage(),
            $refusal->getMessage(),
        );
        static::assertSame(1, $refusal->rowIndex);
    }

    public function test_batches_of_a_header_only_source_are_empty(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        static::assertSame(
            [],
            CSVFixtureContext::batches(
                CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n")),
                schema(int_schema('id'), str_schema('absent')),
                10,
            ),
        );
    }

    /**
     * @return Generator<string, array{string, Schema, positive-int}>
     */
    public static function batch_parity_cases(): Generator
    {
        foreach (CSVFixtureContext::fixtures() as $label => [$fixture]) {
            // the first 1000 records (orders_flow.csv has 10000, one per line) in about five batches: batch
            // boundaries without the cost of typing every record twice
            $content = implode("\n", array_slice(
                explode("\n", (string) file_get_contents(CSVFixtureContext::path($fixture))),
                0,
                1001,
            ));

            yield $label => [
                $content,
                CSVFixtureContext::inferPhp(new SchemaInference(), $fixture),
                max(2, intdiv(substr_count($content, "\n"), 5)),
            ];
        }

        $idName = schema(int_schema('id'), str_schema('name'));

        yield 'missing not null column' => ["id\n1\n", $idName, 2];
        yield 'refused value over absence' => ["id\n1\nx\n", $idName, 2];
        yield 'refusal tie' => ["a,b\nx,y\n", schema(int_schema('b'), int_schema('a')), 2];
        yield 'empty to null under not null' => ["id,name\n1,a\n2,\n", $idName, 2];
        yield 'duplicate header' => ["id,name,name\n1,a,b\n2,c,d\n", $idName, 2];
        yield 'ragged row' => [
            "id,name\n1\n2,b,extra\n",
            schema(int_schema('id'), str_schema('name', nullable: true)),
            2,
        ];
        yield 'numeric header' => ["1,2\na,3\n", schema(str_schema('1'), int_schema('2')), 2];
        yield 'refusal in the second batch' => ["id,name\n1,a\n2,b\n3,c\nx,d\n", $idName, 2];
    }

    /**
     * @param positive-int $batchSize
     */
    #[DataProvider('batch_parity_cases')]
    public function test_native_and_php_batches_are_identical(string $content, Schema $schema, int $batchSize): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        static::assertSame(
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openPhpStream(CSVFixtureContext::content($content)),
                $schema,
                $batchSize,
            ),
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openNativeStream(CSVFixtureContext::content($content)),
                $schema,
                $batchSize,
            ),
        );
    }

    /**
     * @param positive-int $chunkSize
     */
    #[TestWith([1, "id,note\n1,\"a\nb\"\n2,\"c\nd\"\n3,e\n"])]
    #[TestWith([7, "id,note\n1,\"a\nb\"\n2,\"c\nd\"\n3,e\n"])]
    #[TestWith([4096, "id,note\n1,\"a\nb\"\n2,\"c\nd\"\n3,e\n"])]
    #[TestWith([7, "id,note\n1,\"a\nb\"\nx,\"a record that crosses\nthe chunk boundary\"\n"])]
    public function test_batches_are_identical_across_chunk_sizes(int $chunkSize, string $content): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $schema = schema(int_schema('id'), str_schema('note'));

        static::assertSame(
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openNativeStream(
                    new LengthCapturingSourceStream($content, path('s3://bucket/a.csv')),
                ),
                $schema,
                2,
            ),
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openNativeStream(
                    new LengthCapturingSourceStream($content, path('s3://bucket/a.csv')),
                    (new CSVReadOptions())->withCharactersReadInLine($chunkSize),
                ),
                $schema,
                2,
            ),
        );
    }

    public function test_batches_hand_every_column_to_the_backend(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $backend = new SpyBackend();

        CSVFixtureContext::batches(
            CSVFixtureContext::openNative('five_rows.csv'),
            schema(int_schema('id'), str_schema('name')),
            2,
            $backend,
        );

        static::assertSame(2 * 3, $backend->adopts());
    }
}
