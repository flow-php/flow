<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingSourceStream;
use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\TypeFloor;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\RangeSourceStream;
use Flow\Filesystem\Telemetry\FilesystemTelemetryAttributes;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\Attributes\TestWith;

use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function extension_loaded;
use function file_get_contents;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;
use function implode;
use function iterator_to_array;
use function strlen;

final class RustCSVOpenSourceTest extends FlowTestCase
{
    public function test_close_closes_the_stream_once(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        CSVFixtureContext::openNative('two_rows.csv', $counting)->close();

        static::assertSame(1, $counting->closedStreams());
    }

    public function test_columns_of_an_empty_source_is_empty(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = CSVFixtureContext::openNative('two_columns.csv', options: new CSVReadOptions(withHeader: false));

        try {
            static::assertSame(['e00', 'e01'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_only_the_rows_of_records_that_are_not_corrupted(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = CSVFixtureContext::openNative('ragged.csv');

        try {
            static::assertSame([['id' => '1', 'name' => 'a', 'v' => '10']], iterator_to_array($open->records(), false));
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_one_logical_record_per_iteration(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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

    public function test_a_traced_local_stream_reads_the_same_records_and_records_stream_metrics(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $spanProcessor = memory_span_processor(void_exporter());
        $traced = new TraceableFilesystem(
            new NativeLocalFilesystem(),
            FilesystemTelemetryConfigMother::create($spanProcessor),
        );

        $untraced = CSVFixtureContext::openNative('two_rows.csv');
        $open = CSVFixtureContext::openNative('two_rows.csv', $traced);

        static::assertSame(iterator_to_array($untraced->records(), false), iterator_to_array($open->records(), false));

        $open->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.read', $spans[0]->name());
        static::assertGreaterThan(0, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ]);
    }

    public function test_a_remote_stream_is_read_in_steps_of_characters_read_in_line(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $stream = new LengthCapturingSourceStream("id,name\n1,a\n", path('s3://bucket/a.csv'));

        CSVFixtureContext::records(CSVFixtureContext::openNativeStream($stream));

        static::assertSame([1 << 15], $stream->capturedIterateLengths);
    }

    #[TestWith(['multiline_header.csv'])]
    #[TestWith(['with_utf8_bom.csv'])]
    #[TestWith(['semicolon.csv'])]
    #[TestWith(['single_quotes_csv.csv'])]
    public function test_columns_resolves_the_same_header_on_both_paths(string $fixture): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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

    /**
     * @param null|int<0, max> $rowBudget
     */
    #[TestWith([2, 2])]
    #[TestWith([null, 5])]
    #[TestWith([10, 5])]
    public function test_sniff_folds_at_most_the_row_budget(?int $rowBudget, int $rows): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = CSVFixtureContext::openNative('five_rows.csv');

        try {
            static::assertSame(
                $rows,
                $open->sniff(
                    ['id', 'name'],
                    $rowBudget,
                    new StringTypeNarrower(InferredTypes::default()->toArray()),
                )->rows(),
            );
        } finally {
            $open->close();
        }
    }

    public function test_sniff_refuses_a_negative_row_budget_like_the_php_lane(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = CSVFixtureContext::openNative('five_rows.csv');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row budget must be 0 or more. Use null for every row.');

        try {
            // @mago-expect analysis:invalid-argument
            $open->sniff(['id', 'name'], -1, new StringTypeNarrower(InferredTypes::default()->toArray()));
        } finally {
            $open->close();
        }
    }

    public function test_sniff_folds_date_and_time_zone_columns_like_observing_the_records(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $contents = "at,day,zone,n\n2024-01-01 10:00:00,2024-01-01,Europe/Warsaw,\n2024-02-02 11:00:00,2024-02-02,+02:00,1\n";
        $inference = infer_schema()->build();
        $typer = new StringTypeNarrower($inference->candidates()->toArray());
        $names = ['at', 'day', 'zone', 'n'];
        $observed = ColumnTypes::sniff(
            $names,
            CSVFixtureContext::openNativeStream(
                new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
            )->records(),
            null,
            $typer,
        );
        $sniffed = CSVFixtureContext::openNativeStream(
            new LengthCapturingSourceStream($contents, path('s3://bucket/a.csv')),
        )->sniff($names, null, $typer);

        static::assertEquals(
            $observed->folded()->schema(new TypeFloor($inference->candidates())),
            $sniffed->folded()->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_columns_of_a_header_without_a_trailing_newline(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $header = CSVFixtureContext::openPhp($fixture);
        $names = $header->columns();
        $header->close();
        $php = CSVFixtureContext::openPhp($fixture);
        $native = CSVFixtureContext::openNative($fixture);
        $typer = new StringTypeNarrower(infer_schema()->build()->candidates()->toArray());

        try {
            $php->sniff($names, 2, $typer);
            $native->sniff($names, 2, $typer);

            static::assertSame($php->producedRows(), $native->producedRows());
            static::assertSame($php->producedBytes(), $native->producedBytes());
        } finally {
            $php->close();
            $native->close();
        }
    }

    public function test_batches_are_batch_size_rows(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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

    public function test_batches_hand_up_a_record_missing_a_not_null_column(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n1\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [[
                3,
                2,
                'column "'
                    . ColumnMismatchException::missingColumn('name')->column
                    . '"'
                    . ColumnMismatchException::missingColumn('name')->detail,
                '1',
                ['1'],
            ]],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_hand_up_corrupted_records_in_source_order(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id\n1\nx\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [
                [
                    3,
                    2,
                    'column "'
                        . ColumnMismatchException::missingColumn('name')->column
                        . '"'
                        . ColumnMismatchException::missingColumn('name')->detail,
                    '1',
                    ['1'],
                ],
                [
                    5,
                    3,
                    'column "'
                        . ColumnMismatchException::valueDoesNotMatch('id', 'integer', 'x')->column
                        . '"'
                        . ColumnMismatchException::valueDoesNotMatch('id', 'integer', 'x')->detail,
                    'x',
                    ['x'],
                ],
            ],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_refusal_tie_goes_to_the_first_definition(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("a,b\nx,y\n")),
            schema(int_schema('b'), int_schema('a')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [[
                4,
                2,
                'column "'
                    . ColumnMismatchException::valueDoesNotMatch('b', 'integer', 'y')->column
                    . '"'
                    . ColumnMismatchException::valueDoesNotMatch('b', 'integer', 'y')->detail,
                'x,y',
                ['x', 'y'],
            ]],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_hand_up_an_empty_to_null_null_under_not_null(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openNativeStream(CSVFixtureContext::content("id,name\n1,a\n2,\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [
                [
                    12,
                    3,
                    'column "'
                        . ColumnMismatchException::valueDoesNotMatch('name', 'string', null)->column
                        . '"'
                        . ColumnMismatchException::valueDoesNotMatch('name', 'string', null)->detail,
                    '2,',
                    ['2', ''],
                ],
                [['id' => 1, 'name' => 'a']],
            ],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_of_a_header_only_source_are_empty(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
     * @return Generator<string, array{0: string, 1: Schema, 2: positive-int, 3?: CSVReadOptions, 4?: ?SchemaInference, 5?: int<0, max>}>
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

        $idNullableName = schema(int_schema('id'), str_schema('name', nullable: true));

        yield 'bom, crlf, multiline, blank lines and corrupted records' => [
            "\xEF\xBB\xBFid,name\r\n1,\"a\r\nb\"\r\n\r\n2\r\n3,c,x\r\nx,d\r\n4,",
            $idNullableName,
            2,
        ];
        yield 'a cast refusal before a decode refusal in one batch' => ["id,name\nx,a\n2,b,c\n3,d\n", $idName, 10];
        yield 'missing columns as null' => [
            "id,name\n1\n2,b\n3,c,x\n",
            $idNullableName,
            2,
            (new CSVReadOptions())->withMissingColumnsAsNull(true),
        ];
        yield 'refused past the sample' => [
            "id,name\n1,a\n2,b\nx,c\n",
            $idName,
            2,
            new CSVReadOptions(),
            new SchemaInference(2),
        ];
        yield 'window past the first byte' => [
            "e00,e01\n1,a\n2\n3,c\n",
            schema(str_schema('e00'), str_schema('e01', nullable: true)),
            2,
            (new CSVReadOptions())->withHeader(false),
            null,
            8,
        ];
    }

    /**
     * @param positive-int $batchSize
     * @param int<0, max> $windowStart
     */
    #[DataProvider('batch_parity_cases')]
    public function test_native_and_php_batches_are_identical(
        string $content,
        Schema $schema,
        int $batchSize,
        CSVReadOptions $options = new CSVReadOptions(),
        ?SchemaInference $inference = null,
        int $windowStart = 0,
    ): void {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $stream = static fn(): SourceStream => $windowStart === 0
            ? CSVFixtureContext::content($content)
            : new RangeSourceStream(CSVFixtureContext::content($content), $windowStart, strlen($content));

        static::assertSame(
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openPhpStream($stream(), $options),
                $schema,
                $batchSize,
                inference: $inference,
            ),
            CSVFixtureContext::batchesOutcome(
                CSVFixtureContext::openNativeStream($stream(), $options),
                $schema,
                $batchSize,
                inference: $inference,
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
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
