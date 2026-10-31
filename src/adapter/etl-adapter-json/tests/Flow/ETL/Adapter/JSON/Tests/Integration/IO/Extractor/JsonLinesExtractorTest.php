<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Closure;
use Flow\ETL\Adapter\JSON\IO\Extractor\JsonExtractor;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Context\SplitCoverage;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function abs;
use function array_keys;
use function array_map;
use function array_sum;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function iterator_to_array;
use function max;
use function sprintf;
use function str_repeat;

final class JsonLinesExtractorTest extends FlowTestCase
{
    public function test_a_key_first_seen_after_the_inference_sample_is_a_schema_error(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('"c"');

        ExtractedRows::of(from_json_lines(
            path('memory://source.jsonl'),
            MemoryFiles::with([
                'memory://source.jsonl' => "{\"a\":1,\"b\":2}\n{\"a\":3,\"b\":4}\n{\"a\":5,\"b\":6,\"c\":7}\n",
            ]),
        )->inferSchema(infer_schema()->sampleSize(2)));
    }

    public function test_a_value_that_does_not_cast_to_the_declared_type_is_a_corrupted_record(): void
    {
        try {
            ExtractedRows::of(from_json_lines(
                path('memory://source.jsonl'),
                MemoryFiles::with([
                    'memory://source.jsonl' => "{\"id\":1,\"amount\":10.5}\n{\"id\":2,\"amount\":\"N/A\"}\n",
                ]),
            )->withSchema(schema(int_schema('id'), float_schema('amount', true))));
            static::fail('Expected the corrupted record to be refused');
        } catch (CorruptedRecordException $e) {
            static::assertSame(['memory://source.jsonl', 23, 2, '{"id":2,"amount":"N/A"}', null], [
                $e->uri,
                $e->offset,
                $e->lineNumber,
                $e->record,
                $e->fields,
            ]);
            static::assertStringContainsString('"amount"', $e->reason);
        }
    }

    public function test_extracting_jsonl_from_local_file_stream(): void
    {
        $rows = data_frame(config_builder())
            ->read(from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl')->withMetadataColumns(true))
            ->fetch();

        foreach ($rows->toArray() as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                    '_input_file_uri',
                ],
                array_keys($row),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(int_schema('id'), str_schema('value'));

        $extractor = from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl')
            ->withSchema($schema)
            ->withMetadataColumns(true);

        df(config_builder())->read($extractor)->run();
        df(config_builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_extracting_jsonl_from_local_file_stream_using_pointer(): void
    {
        $rows = data_frame()
            ->read(from_json_lines(__DIR__ . '/../../../Fixtures/nested_timezones.jsonl')->withPointer(
                '/timezones',
                true,
            ))
            ->fetch();

        foreach ($rows->toArray() as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys(type_array()->assert($row['/timezones'])),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_jsonl_from_local_file_stream_with_schema(): void
    {
        $schema = df()->read(from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl'))->schema();

        $rows = df()
            ->read(from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl')->withSchema($schema))
            ->fetch();

        foreach ($rows->toArray() as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys($row),
            );
        }

        static::assertSame(247, $rows->count());
        static::assertEquals($schema, $rows->schema());
        static::assertSame(<<<'SCHEMA'
            schema
            |-- timezones: ?list<string>
            |-- latlng: ?list<float>
            |-- name: ?string
            |-- country_code: ?string
            |-- capital: ?string

            SCHEMA, schema_to_ascii($schema));
    }

    public function test_extracting_jsonl_from_local_file_string_uri(): void
    {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../../Fixtures/timezones.jsonl'));

        $total = 0;

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            foreach ($rows->toArray() as $row) {
                static::assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',
                    ],
                    array_keys($row),
                );
            }
            $total += $rows->count();
        }

        static::assertSame(247, $total);
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['id' => 2, 'value' => 'b', 'date' => null],
            ],
            df()
                ->read(from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl')->withSchema(schema(
                    int_schema('id'),
                    str_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_limit(): void
    {
        $extractor = from_json_lines(path(__DIR__ . '/../../../Fixtures/timezones.jsonl'));
        $extractor->withBatchSize(1);

        self::assertExtractedRowsCount(2, $extractor, flow_context(config()), criteria: new Criteria(2));
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('name'), str_schema('_input_file_uri')),
            from_json_lines(__DIR__ . '/../../../Fixtures/parity_people.jsonl')
                ->withSchema(schema(str_schema('name')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_schema_is_inferred_when_it_was_not_declared(): void
    {
        static::assertSame(<<<'SCHEMA'
            schema
            |-- timezones: ?list<string>
            |-- latlng: ?list<float>
            |-- name: ?string
            |-- country_code: ?string
            |-- capital: ?string

            SCHEMA, schema_to_ascii(from_json_lines(JsonFixtureContext::path('timezones.jsonl'))->schema()));
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            from_json_lines(__DIR__ . '/../../../Fixtures/parity_people.jsonl')
                ->withSchema(schema(str_schema('name')))
                ->schema(),
        );
    }

    public function test_signal_stop(): void
    {
        $generator = FlowTestCase::extracted(
            from_json_lines(path(__DIR__ . '/../../../Fixtures/timezones.jsonl'))->withBatchSize(100),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_every_batch_carries_the_inferred_schema(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('timezones.jsonl'));
        $expected = $extractor->schema();
        $sizes = [];

        foreach (FlowTestCase::extracted(
            $extractor->withBatchSize(3),
            flow_context()->backend(),
            new Criteria(),
        ) as $rows) {
            static::assertTrue($rows->schema()->isSame($expected));
            $sizes[] = $rows->count();
        }

        static::assertLessThanOrEqual(3, max($sizes));
        static::assertContains(3, $sizes);
        static::assertSame(
            ExtractedRows::of(from_json_lines(JsonFixtureContext::path('timezones.jsonl')))->count(),
            array_sum($sizes),
        );
    }

    public function test_the_inferred_schema_is_memoised_across_schema_and_extract(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'), filesystem: $counting);

        $extractor->schema();
        iterator_to_array(FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()));

        static::assertSame(2, $counting->readFromCalls, 'sample + read');
        static::assertSame(2, $counting->closedStreams());
        static::assertSame(1, $counting->listCalls);
    }

    public function test_a_declared_schema_opens_each_file_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(
            JsonFixtureContext::path('five_rows.jsonl'),
            filesystem: $counting,
        )->withSchema(schema(int_schema('id')));

        $extractor->schema();
        iterator_to_array(FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()));

        static::assertSame(1, $counting->readFromCalls);
    }

    /**
     * @param Closure(JsonExtractor): void $setter
     */
    #[DataProvider('shapeChangingSetters')]
    public function test_a_shape_changing_setter_drops_the_inferred_schema(Closure $setter): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(JsonFixtureContext::path('nested_timezones.jsonl'), filesystem: $counting);

        $extractor->schema();
        $cold = $counting->readFromCalls;

        $setter($extractor);
        $extractor->schema();

        static::assertGreaterThan($cold, $counting->readFromCalls);
    }

    /**
     * @return Generator<string, array{Closure(JsonExtractor): void}>
     */
    public static function shapeChangingSetters(): Generator
    {
        yield 'withPointer' => [static fn(JsonExtractor $e) => $e->withPointer('/timezones', true)];
        yield 'inferSchema' => [static fn(JsonExtractor $e) => $e->inferSchema(infer_schema()->allStrings())];
    }

    public function test_infer_schema_drops_the_inferred_schema_and_all_strings_floors_it(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('timezones.jsonl'));

        static::assertSame('list<float>', $extractor->schema()->get('latlng')->type()->toString());

        $extractor->inferSchema(infer_schema()->allStrings());

        static::assertSame(<<<'SCHEMA'
            schema
            |-- timezones: ?list<string>
            |-- latlng: ?list<string>
            |-- name: ?string
            |-- country_code: ?string
            |-- capital: ?string

            SCHEMA, schema_to_ascii($extractor->schema()));
    }

    public function test_a_pointer_that_names_the_column_reshapes_the_inferred_schema(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('nested_timezones.jsonl'))->withPointer('/timezones');

        static::assertSame(
            ['timezones', 'latlng', 'name', 'country_code', 'capital'],
            array_keys($extractor->schema()->definitions()),
        );

        $extractor->withPointer('/timezones', true);

        static::assertSame(['/timezones'], array_keys($extractor->schema()->definitions()));
        static::assertStringStartsWith('structure', $extractor->schema()->get('/timezones')->type()->toString());
    }

    public function test_metadata_columns_keep_the_inferred_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'), filesystem: $counting);

        $extractor->schema();
        $cold = $counting->readFromCalls;

        $extractor->withMetadataColumns(true);
        $names = array_keys($extractor->schema()->definitions());

        static::assertSame($cold, $counting->readFromCalls);
        static::assertSame(['id', '_input_file_uri'], $names);
    }

    public function test_sample_size_bounds_the_inferred_schema(): void
    {
        static::assertSame(
            'integer',
            from_json_lines(JsonFixtureContext::path('misfit.jsonl'))
                ->inferSchema(infer_schema()->sampleSize(3))
                ->schema()
                ->get('id')
                ->type()
                ->toString(),
        );

        static::assertSame(
            'string',
            from_json_lines(JsonFixtureContext::path('misfit.jsonl'))->schema()->get('id')->type()->toString(),
            'the whole file widens integer to string',
        );

        static::assertSame(
            [['id' => '1'], ['id' => '2'], ['id' => '3'], ['id' => 'n/a']],
            df(config())
                ->read(from_json_lines(JsonFixtureContext::path('misfit.jsonl')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_value_past_the_sample_is_reported(): void
    {
        $this->expectException(InferredSchemaException::class);

        df(config())
            ->read(
                from_json_lines(JsonFixtureContext::path('misfit.jsonl'))->inferSchema(infer_schema()->sampleSize(3)),
            )
            ->fetch();
    }

    public function test_files_to_sniff_bounds_the_sources_opened(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        static::assertSame(
            ['id', 'name'],
            array_keys(
                from_json_lines(JsonFixtureContext::path('extra_key/*.jsonl'), filesystem: $counting)
                    ->inferSchema(infer_schema()->filesToSniff(1))
                    ->schema()
                    ->definitions(),
            ),
        );
        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_a_key_first_seen_past_the_sample_is_a_schema_error(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('column "extra" is not declared by the schema');

        df(config())
            ->read(
                from_json_lines(JsonFixtureContext::path('extra_key/*.jsonl'))
                    ->inferSchema(infer_schema()->filesToSniff(1)),
            )
            ->fetch();
    }

    public function test_union_by_name_reads_diverging_columns_as_one_wider_schema(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('extra_key/*.jsonl'))
            ->inferSchema(infer_schema()->unionByName());

        static::assertSame(['id', 'name', 'extra'], array_keys($extractor->schema()->definitions()));
        static::assertSame(
            [
                ['id' => 1, 'name' => 'a', 'extra' => null],
                ['id' => 2, 'name' => 'b', 'extra' => null],
                ['id' => 3, 'name' => 'c', 'extra' => true],
            ],
            df(config())->read($extractor)->fetch()->toArray(),
        );
    }

    public function test_a_key_missing_past_the_sample_is_null(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            df(config())
                ->read(
                    from_json_lines(JsonFixtureContext::path('missing_key/*.jsonl'))
                        ->inferSchema(infer_schema()->filesToSniff(1)),
                )
                ->fetch()
                ->toArray(),
        );
    }

    public function test_an_empty_document_refuses_to_infer_a_schema_and_extracts_nothing(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('empty.jsonl'));

        static::assertSame(
            [],
            iterator_to_array(
                FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()),
                false,
            ),
        );

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_json_lines() has nothing to infer a schema from: no file matching "'
            . path_real(JsonFixtureContext::path('empty.jsonl'))->uri()
            . '" carries a column. Declare the schema with ->withSchema(...).',
        );

        $extractor->schema();
    }

    /**
     * @param int<1, max>|-1 $filesToSniff
     */
    #[TestWith(['glob_with_empty', 10])]
    #[TestWith(['glob_empty_first', 10])]
    #[TestWith(['glob_empty_first', 1])]
    public function test_an_empty_document_in_a_glob_is_skipped(string $fixture, int $filesToSniff): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path($fixture . '/*.jsonl'))
            ->inferSchema(infer_schema()->filesToSniff($filesToSniff));

        static::assertSame(<<<'SCHEMA'
            schema
            |-- id: ?integer
            |-- name: ?string

            SCHEMA, schema_to_ascii($extractor->schema()));
        static::assertSame([['id' => 1, 'name' => 'a']], df(config())->read($extractor)->fetch()->toArray());
    }

    /**
     * @param 'memory'|'native' $filesystem
     */
    #[TestWith(['native'])]
    #[TestWith(['memory'])]
    public function test_a_blank_line_is_skipped_by_schema_and_read(string $filesystem): void
    {
        $extractor = JsonFixtureContext::linesExtractor('blank_line.jsonl', $filesystem);

        static::assertSame(<<<'SCHEMA'
            schema
            |-- id: ?integer

            SCHEMA, schema_to_ascii($extractor->schema()));
        static::assertSame([['id' => 1], ['id' => 2]], df(config())->read($extractor)->fetch()->toArray());
    }

    public function test_an_empty_listing_refuses_to_infer_a_schema_without_opening_a_file(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        try {
            from_json_lines(JsonFixtureContext::path('no_such_dir/*.jsonl'), filesystem: $counting)->schema();
            static::fail('an empty listing must refuse to infer a schema');
        } catch (InferredSchemaException $e) {
            static::assertSame(
                'from_json_lines() has nothing to infer a schema from: no file matching "'
                . path_real(JsonFixtureContext::path('no_such_dir/*.jsonl'))->uri()
                . '" carries a column. Declare the schema with ->withSchema(...).',
                $e->getMessage(),
            );
        }

        static::assertSame(0, $counting->readFromCalls);
    }

    public function test_partition_columns_are_not_typed_by_the_sample(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('partition_collision/group=*/data.jsonl'));

        static::assertInstanceOf(StringDefinition::class, $extractor->schema()->get('group'));
        static::assertSame([['id' => 1, 'group' => '1']], df(config())->read($extractor)->fetch()->toArray());
    }

    #[TestWith(['limit'])]
    #[TestWith(['stop'])]
    #[TestWith(['hydrator-throws'])]
    public function test_a_stream_is_closed_when_the_read_stops_early(string $mode): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        if ($mode === 'hydrator-throws') {
            $extractor = from_json_lines(
                JsonFixtureContext::path('misfit.jsonl'),
                filesystem: $counting,
            )->inferSchema(infer_schema()->sampleSize(3));
        } else {
            $extractor = from_json_lines(JsonFixtureContext::path('timezones.jsonl'), filesystem: $counting);
        }

        if ($mode === 'limit') {
            iterator_to_array(FlowTestCase::extracted(
                $extractor,
                flow_context(config())->backend(),
                new Criteria(limit: 2),
            ));
        }

        if ($mode === 'stop') {
            $generator = FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria());
            static::assertTrue($generator->valid());
            $generator->send(Signal::STOP);
        }

        if ($mode === 'hydrator-throws') {
            try {
                iterator_to_array(FlowTestCase::extracted(
                    $extractor,
                    flow_context(config())->backend(),
                    new Criteria(),
                ));

                static::fail('expected ' . SchemaMismatchException::class);
            } catch (InferredSchemaException) {
            }
        }

        static::assertGreaterThan(0, $counting->readFromCalls);
        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_source_is_the_path_it_was_given(): void
    {
        static::assertSame(
            path_real(JsonFixtureContext::path('five_rows.jsonl'))->uri(),
            from_json_lines(JsonFixtureContext::path('five_rows.jsonl'))->source()->uri(),
        );
    }

    public function test_the_filesystem_must_serve_the_paths_protocol(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pass the filesystem that handles this scheme');

        from_json_lines(JsonFixtureContext::path('five_rows.jsonl'), filesystem: memory_filesystem());
    }

    public function test_partition_types_keep_the_inferred_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(
            JsonFixtureContext::path('partition_collision/group=*/data.jsonl'),
            filesystem: $counting,
        );

        $extractor->schema();
        $cold = $counting->readFromCalls;

        static::assertSame(
            'integer',
            $extractor
                ->partitionTypes(partition_types(group: type_integer()))
                ->schema()
                ->get('group')
                ->type()
                ->toString(),
        );
        static::assertSame($cold, $counting->readFromCalls);
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl')->isRepeatable());
    }

    /**
     * @param Closure(JsonExtractor): void $setter
     */
    #[DataProvider('shapeChangingSetters')]
    public function test_a_shape_changing_setter_drops_the_sample(Closure $setter): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);

        $setter($extractor);

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
    }

    public function test_a_declared_schema_declares_unknown_rows(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'))->withSchema(schema(int_schema('id')));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(45), $extractor->statistics()->size);
    }

    public function test_a_file_shorter_than_the_sample_declares_an_exact_row_count(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_a_member_without_a_size_makes_the_size_unknown(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('glob_with_empty/*.jsonl'))
            ->inferSchema(infer_schema()->sampleSize(1));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->size);
    }

    public function test_a_narrow_first_row_does_not_skew_the_estimate(): void
    {
        $lines = "{\"a\":1}\n";

        for ($i = 0; $i < 99; $i++) {
            $lines .= sprintf("{\"a\":\"%012d\"}\n", $i);
        }

        $extractor = from_json_lines(
            path('memory://source.jsonl'),
            MemoryFiles::with([
                'memory://source.jsonl' => $lines,
            ]),
        )->inferSchema(infer_schema()->sampleSize(20));
        $extractor->schema();

        $rows = $extractor->statistics()->rows;

        static::assertEquals(Cardinality::approximately(103, Cardinality::DEFAULT_RELATIVE_ERROR), $rows);
        static::assertLessThanOrEqual($rows->relativeError, abs(103 - 100) / 100);
    }

    public function test_extract_samples_like_schema_does(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'));

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false);

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_it_declares_the_listed_byte_total_exactly(): void
    {
        static::assertEquals(
            Cardinality::exact(27),
            from_json_lines(
                path('memory://dir/*.jsonl'),
                MemoryFiles::with([
                    'memory://dir/a.jsonl' => "{\"id\":1}\n",
                    'memory://dir/b.jsonl' => "{\"id\":2}\n{\"id\":3}\n",
                ]),
            )->statistics()->size,
        );
    }

    public function test_it_estimates_rows_from_bytes_and_the_mean_sampled_row(): void
    {
        $extractor = from_json_lines(
            path('memory://source.jsonl'),
            MemoryFiles::with([
                'memory://source.jsonl' => str_repeat("{\"i\":1234}\n", 100),
            ]),
        )->inferSchema(infer_schema()->sampleSize(10));
        $extractor->schema();

        static::assertEquals(
            Cardinality::approximately(100, Cardinality::DEFAULT_RELATIVE_ERROR),
            $extractor->statistics()->rows,
        );
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'), filesystem: $filesystem);
        $extractor->schema();

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_statistics_before_schema_declares_unknown_rows(): void
    {
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'));

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);

        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_the_listing_is_read_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json_lines(JsonFixtureContext::path('five_rows.jsonl'), filesystem: $filesystem);

        $extractor->statistics();
        $extractor->statistics();

        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $backend = new SpyBackend();

        iterator_to_array(
            FlowTestCase::extracted(
                from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl'),
                flow_context(config_builder()->backend($backend)->build())->backend(),
                new Criteria(),
            ),
            false,
        );

        static::assertGreaterThanOrEqual(1, $backend->builders() + $backend->adopts());
    }

    public function test_extract_split_stops_on_the_stop_signal(): void
    {
        $extractor = from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl');
        $extractor->withBatchSize(1);
        $generator = FlowTestCase::extracted(
            $extractor,
            flow_context()->backend(),
            (new Criteria())->withSplit($extractor->splits(new Criteria(), 1)[0]),
        );
        $totalRows = 0;

        foreach ($generator as $rows) {
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $totalRows);
    }

    public function test_splits_cover_every_row_exactly_once(): void
    {
        static::assertSame(
            2,
            array_sum(SplitCoverage::rowCountPerSplit(
                from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl'),
                flow_context(),
                1,
            )),
        );
    }

    public function test_a_sized_file_is_cut_into_byte_windows(): void
    {
        $extractor = from_json_lines(__DIR__ . '/../../../Fixtures/timezones.jsonl');
        $splits = $extractor->splits(new Criteria(), 8192);

        static::assertCount(4, $splits);
        static::assertSame([0, 8192, 16384, 24576], array_map(static fn(FileSplit $s): int => $s->start, $splits));
        static::assertSame(
            ExtractedRows::of($extractor)->toArray(),
            SplitCoverage::rowsFromSplits($extractor, flow_context(), 8192)->toArray(),
        );
    }

    public function test_splits_preserve_source_order(): void
    {
        static::assertEquals(
            ExtractedRows::of(from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl')),
            SplitCoverage::rowsFromSplits(
                from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl'),
                flow_context(),
                1,
            ),
        );
    }

    public function test_a_pushed_path_filter_prunes_the_files_splits_are_cut_from(): void
    {
        $extractor = from_json_lines(__DIR__ . '/../../../Fixtures/cross_stream/*/data.jsonl');

        static::assertEquals(
            [new FileSplit($extractor->source(), 0, null)],
            $extractor->splits((new Criteria())->withPathFilter(new RejectingFilter()), 1),
        );
    }

    public function test_corrupted_records_are_routed_to_the_reject_loader(): void
    {
        $rejects = [];

        $rows = df()
            ->read(
                from_json_lines(
                    path('memory://source.jsonl'),
                    MemoryFiles::with([
                        'memory://source.jsonl' => "{\"id\":1}\n{\"id\":\"x\"}\n{\"id\":3}\n",
                    ]),
                )
                    ->withSchema(schema(int_schema('id')))
                    ->withCorruptedRecords(to_array($rejects)),
            )
            ->fetch();

        static::assertSame([1, 3], $rows->reduceToArray('id'));
        static::assertSame(
            [[
                'uri' => 'memory://source.jsonl',
                'offset' => 9,
                'line' => 2,
                'reason' => 'column "id": could not convert \'x\' (string) to integer',
                'record' => '{"id":"x"}',
                'fields' => null,
            ]],
            $rejects,
        );
    }
}
