<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Closure;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_sum;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function iterator_to_array;
use function max;

final class JsonExtractorTest extends FlowTestCase
{
    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(int_schema('id'), str_schema('value'));

        $extractor = from_json(
            __DIR__ . '/../../Fixtures/cross_stream/*/data.json',
            schema: $schema,
        )->withMetadataColumns(true);

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_extracting_json_from_local_file_stream(): void
    {
        $rows = data_frame(Config::builder())
            ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json')->withMetadataColumns(true))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                    '_input_file_uri',
                ],
                array_keys($row->toArray()),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_using_pointer(): void
    {
        $rows = data_frame()
            ->read(from_json(__DIR__ . '/../../Fixtures/nested_timezones.json')->withPointer('/timezones', true))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys(type_array()->assert($row->get('/timezones'))),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_with_schema(): void
    {
        $rows = df()
            ->read(from_json(
                __DIR__ . '/../../Fixtures/timezones.json',
                schema: $schema = df()->read(from_json(__DIR__ . '/../../Fixtures/timezones.json'))->schema(),
            ))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys($row->toArray()),
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

    public function test_extracting_json_from_local_file_string_uri(): void
    {
        $extractor = from_json(path_real(__DIR__ . '/../../Fixtures/timezones.json'));

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',
                    ],
                    array_keys($row->toArray()),
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
                ->read(from_json(__DIR__ . '/../../Fixtures/cross_stream/*/data.json', schema: schema(
                    int_schema('id'),
                    str_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_limit(): void
    {
        $extractor = from_json(path(__DIR__ . '/../../Fixtures/timezones.json'));
        $extractor->withBatchSize(1)->pushLimit(2);

        self::assertExtractedRowsCount(2, $extractor, flow_context(config()));
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('order_id'), str_schema('_input_file_uri')),
            from_json(__DIR__ . '/../../Fixtures/orders_flow.json', schema: schema(str_schema('order_id')))
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

            SCHEMA, schema_to_ascii(from_json(JsonFixtureContext::path('timezones.json'))->schema()));
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('order_id')),
            from_json(__DIR__ . '/../../Fixtures/orders_flow.json', schema: schema(str_schema('order_id')))->schema(),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_json(path(__DIR__ . '/../../Fixtures/timezones.json'));

        $generator = $extractor->extract(flow_context(config()));

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
        $extractor = from_json(JsonFixtureContext::path('timezones.json'));
        $expected = $extractor->schema();
        $sizes = [];

        foreach ($extractor->withBatchSize(3)->extract(flow_context()) as $rows) {
            static::assertTrue($rows->schema()->isSame($expected));
            $sizes[] = $rows->count();
        }

        static::assertLessThanOrEqual(3, max($sizes));
        static::assertContains(3, $sizes);
        static::assertSame(
            ExtractedRows::of(from_json(JsonFixtureContext::path('timezones.json')))->count(),
            array_sum($sizes),
        );
    }

    public function test_the_inferred_schema_is_memoised_across_schema_and_extract(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json(JsonFixtureContext::path('five_rows.json'), filesystem: $counting);

        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertSame(2, $counting->readFromCalls, 'sample + read');
        static::assertSame(2, $counting->closedStreams());
        static::assertSame(3, $counting->listCalls);
    }

    public function test_a_declared_schema_opens_each_file_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json(
            JsonFixtureContext::path('five_rows.json'),
            filesystem: $counting,
        )->withSchema(schema(int_schema('id')));

        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertSame(1, $counting->readFromCalls);
    }

    /**
     * @param Closure(JsonExtractor): void $setter
     */
    #[DataProvider('shapeChangingSetters')]
    public function test_a_shape_changing_setter_drops_the_inferred_schema(Closure $setter): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json(JsonFixtureContext::path('nested_timezones.json'), filesystem: $counting);

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
        $extractor = from_json(JsonFixtureContext::path('timezones.json'));

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
        $extractor = from_json(JsonFixtureContext::path('nested_timezones.json'))->withPointer('/timezones');

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
        $extractor = from_json(JsonFixtureContext::path('five_rows.json'), filesystem: $counting);

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
            from_json(JsonFixtureContext::path('misfit.json'))
                ->inferSchema(infer_schema()->sampleSize(3))
                ->schema()
                ->get('id')
                ->type()
                ->toString(),
        );

        static::assertSame(
            'string',
            from_json(JsonFixtureContext::path('misfit.json'))->schema()->get('id')->type()->toString(),
            'the whole file widens integer to string',
        );

        static::assertSame(
            [['id' => '1'], ['id' => '2'], ['id' => '3'], ['id' => 'n/a']],
            df(config())
                ->read(from_json(JsonFixtureContext::path('misfit.json')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_value_past_the_sample_is_reported(): void
    {
        $this->expectException(SchemaMismatchException::class);

        df(config())
            ->read(from_json(JsonFixtureContext::path('misfit.json'))->inferSchema(infer_schema()->sampleSize(3)))
            ->fetch();
    }

    public function test_files_to_sniff_bounds_the_sources_opened(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json(
            JsonFixtureContext::path('extra_key/*.json'),
            filesystem: $counting,
        )->inferSchema(infer_schema()->filesToSniff(1));

        static::assertSame(['id', 'name'], array_keys($extractor->schema()->definitions()));
        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_a_key_first_seen_past_the_sample_is_dropped(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            df(config())
                ->read(
                    from_json(JsonFixtureContext::path('extra_key/*.json'))
                        ->inferSchema(infer_schema()->filesToSniff(1)),
                )
                ->fetch()
                ->toArray(),
        );
    }

    public function test_union_by_name_reads_diverging_columns_as_one_wider_schema(): void
    {
        $extractor = from_json(JsonFixtureContext::path('extra_key/*.json'))
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
                    from_json(JsonFixtureContext::path('missing_key/*.json'))
                        ->inferSchema(infer_schema()->filesToSniff(1)),
                )
                ->fetch()
                ->toArray(),
        );
    }

    #[TestWith(['empty_array.json'])]
    #[TestWith(['array_of_empty_object.json'])]
    #[TestWith(['empty.json'])]
    public function test_an_empty_document_infers_an_empty_schema(string $fixture): void
    {
        $extractor = from_json(JsonFixtureContext::path($fixture));

        static::assertSame([], $extractor->schema()->definitions());
        static::assertSame([], iterator_to_array($extractor->extract(flow_context(config())), false));
    }

    /**
     * @param int<1, max>|-1 $filesToSniff
     */
    #[TestWith(['glob_with_empty', 10])]
    #[TestWith(['glob_empty_first', 10])]
    #[TestWith(['glob_empty_first', 1])]
    public function test_an_empty_document_in_a_glob_is_skipped(string $fixture, int $filesToSniff): void
    {
        $extractor = from_json(JsonFixtureContext::path($fixture . '/*.json'))
            ->inferSchema(infer_schema()->filesToSniff($filesToSniff));

        static::assertSame(<<<'SCHEMA'
            schema
            |-- id: ?integer
            |-- name: ?string

            SCHEMA, schema_to_ascii($extractor->schema()));
        static::assertSame([['id' => 1, 'name' => 'a']], df(config())->read($extractor)->fetch()->toArray());
    }

    public function test_an_empty_listing_infers_an_empty_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        static::assertSame(
            [],
            from_json(JsonFixtureContext::path('no_such_dir/*.json'), filesystem: $counting)->schema()->definitions(),
        );
        static::assertSame(0, $counting->readFromCalls);
    }

    public function test_partition_columns_are_not_typed_by_the_sample(): void
    {
        $extractor = from_json(JsonFixtureContext::path('partition_collision/group=*/data.json'));

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
            $extractor = from_json(
                JsonFixtureContext::path('misfit.json'),
                filesystem: $counting,
            )->inferSchema(infer_schema()->sampleSize(3));
        } else {
            $extractor = from_json(JsonFixtureContext::path('timezones.json'), filesystem: $counting);
        }

        if ($mode === 'limit') {
            $extractor->pushLimit(2);
            iterator_to_array($extractor->extract(flow_context(config())));
        }

        if ($mode === 'stop') {
            $generator = $extractor->extract(flow_context(config()));
            static::assertTrue($generator->valid());
            $generator->send(Signal::STOP);
        }

        if ($mode === 'hydrator-throws') {
            try {
                iterator_to_array($extractor->extract(flow_context(config())));

                static::fail('expected ' . SchemaMismatchException::class);
            } catch (SchemaMismatchException) {
            }
        }

        static::assertGreaterThan(0, $counting->readFromCalls);
        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_source_is_the_path_it_was_given(): void
    {
        static::assertSame(
            path_real(JsonFixtureContext::path('five_rows.json'))->uri(),
            from_json(JsonFixtureContext::path('five_rows.json'))->source()->uri(),
        );
    }

    public function test_the_filesystem_must_serve_the_paths_protocol(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pass the filesystem that handles this scheme');

        from_json(JsonFixtureContext::path('five_rows.json'), filesystem: memory_filesystem());
    }

    public function test_partition_types_keep_the_inferred_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_json(
            JsonFixtureContext::path('partition_collision/group=*/data.json'),
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
        static::assertTrue(from_json(__DIR__ . '/../../Fixtures/timezones.json')->isRepeatable());
    }
}
