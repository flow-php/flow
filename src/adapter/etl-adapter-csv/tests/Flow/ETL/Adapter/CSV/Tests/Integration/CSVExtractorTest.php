<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Closure;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Config;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\OperatingSystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;

use function array_filter;
use function array_keys;
use function array_map;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function sort;

final class CSVExtractorTest extends FlowTestCase
{
    use OperatingSystem;

    public function test_a_hive_null_partition_value_reads_back_as_null(): void
    {
        $dir = __DIR__ . '/var/test_a_hive_null_partition_value_reads_back_as_null';

        df()
            ->read(from_array(
                [['id' => 1, 'year' => null], ['id' => 2, 'year' => '2024']],
                schema(int_schema('id'), str_schema('year', nullable: true)),
            ))
            ->write(to_csv($dir . '/data.csv')->saveMode(overwrite())->partitionBy(partition_by('year')))
            ->run();

        $rows = df()->read(from_csv($dir . '/*/*.csv')->withSchema(schema(int_schema('id'))))->fetch();

        $years = array_map(static fn(Row $row): mixed => $row->get('year'), $rows->all());
        sort($years);

        static::assertSame([null, '2024'], $years);
    }

    public function test_declared_partition_types_reach_the_rows(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')
            ->withSchema(schema(int_schema('id'), str_schema('value')))
            ->partitionTypes(partition_types(group: type_integer()));

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertSame(1, $rows->first()->get('group'));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_partition_columns_are_typed_without_a_declared_schema(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->partitionTypes(
            partition_types(group: type_integer()),
        );

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertSame(1, $rows->first()->get('group'));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_a_source_column_named_input_file_uri_throws_instead_of_being_overwritten(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        df()
            ->read(from_csv(__DIR__ . '/../Fixtures/metadata_column_collision.csv', schema: schema(
                str_schema('_input_file_uri'),
                str_schema('name'),
            ))->withMetadataColumns(true))
            ->fetch();
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('name'), str_schema('_input_file_uri')),
            from_csv(
                __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
                schema: schema(str_schema('name')),
            )
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_characters_read_in_line_must_be_greater_than_zero(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Characters read in line must be greater than 0');

        from_csv(CSVFixtureContext::path('two_rows.csv'))->withCharactersReadInLine(0);
    }

    public function test_source_is_the_path_it_was_built_with(): void
    {
        static::assertSame(
            path_real(CSVFixtureContext::path('two_rows.csv'))->uri(),
            from_csv(CSVFixtureContext::path('two_rows.csv'))->source()->uri(),
        );
    }

    /**
     * The dialect setters are not the detector's single-character guard: a multi-character value degrades the
     * same way it did before inference existed, rather than throwing.
     *
     * @param Closure(CSVExtractor): CSVExtractor $setter
     */
    #[DataProvider('multiCharacterDialectSetters')]
    public function test_a_multi_character_dialect_value_does_not_throw(Closure $setter): void
    {
        static::assertNotSame([], $setter(from_csv(CSVFixtureContext::path('two_rows.csv')))->schema()->definitions());
    }

    /**
     * @return Generator<string, array{Closure(CSVExtractor): CSVExtractor}>
     */
    public static function multiCharacterDialectSetters(): Generator
    {
        yield 'withSeparator' => [static fn(CSVExtractor $e): CSVExtractor => $e->withSeparator('||')];
        yield 'withEnclosure' => [static fn(CSVExtractor $e): CSVExtractor => $e->withEnclosure('||')];
        yield 'withEscape' => [static fn(CSVExtractor $e): CSVExtractor => $e->withEscape('ab')];
    }

    public function test_schema_is_inferred_when_it_was_not_declared(): void
    {
        $schema = from_csv(CSVFixtureContext::path(
            'annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
        ))->schema();

        static::assertSame('integer', $schema->get('Year')->type()->toString());
        static::assertSame('string', $schema->get('Industry_code_NZSIOC')->type()->toString());
        static::assertSame('string', $schema->get('Value')->type()->toString());

        foreach ($schema->definitions() as $definition) {
            static::assertTrue($definition->isNullable(), $definition->entry()->name() . ' must be nullable');
        }
    }

    public function test_every_batch_carries_the_inferred_schema(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('orders_flow.csv'));
        $schema = $extractor->schema();
        $seen = 0;

        foreach ($extractor->extract(flow_context(Config::builder()->extractorBatchSize(3)->build())) as $rows) {
            static::assertTrue($schema->isSame($rows->schema()));
            $seen++;

            if ($seen === 10) {
                break;
            }
        }

        static::assertSame(10, $seen);
    }

    public function test_a_declared_schema_opens_each_file_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('two_rows.csv'), filesystem: $counting)->withSchema(schema(
            int_schema('id'),
            str_schema('name'),
        ));

        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_the_inferred_schema_is_memoised_across_schema_and_extract(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('two_rows.csv'), filesystem: $counting);

        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertSame(3, $counting->readFromCalls, 'header() + sample + read');
        static::assertSame(3, $counting->closedStreams());
        static::assertSame(3, $counting->listCalls);
    }

    /**
     * @param Closure(CSVExtractor): void $setter
     */
    #[DataProvider('shapeChangingSetters')]
    public function test_a_shape_changing_setter_drops_the_inferred_schema(Closure $setter): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('file_with_empty_columns.csv'), filesystem: $counting);

        $extractor->schema();
        $cold = $counting->readFromCalls;

        $setter($extractor);
        $extractor->schema();

        static::assertGreaterThan($cold, $counting->readFromCalls);
    }

    /**
     * @return Generator<string, array{Closure(CSVExtractor): void}>
     */
    public static function shapeChangingSetters(): Generator
    {
        yield 'withBOMRemoval' => [static fn(CSVExtractor $e) => $e->withBOMRemoval(false)];
        yield 'withCharactersReadInLine' => [static fn(CSVExtractor $e) => $e->withCharactersReadInLine(2000)];
        yield 'withEmptyToNull' => [static fn(CSVExtractor $e) => $e->withEmptyToNull(false)];
        yield 'withEnclosure' => [static fn(CSVExtractor $e) => $e->withEnclosure("'")];
        yield 'withEscape' => [static fn(CSVExtractor $e) => $e->withEscape('/')];
        yield 'withHeader' => [static fn(CSVExtractor $e) => $e->withHeader(false)];
        yield 'withSeparator' => [static fn(CSVExtractor $e) => $e->withSeparator(';')];
    }

    public function test_infer_schema_drops_the_inferred_schema_and_all_strings_floors_it(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('file_with_empty_columns.csv'), filesystem: $counting);

        $extractor->schema();
        $cold = $counting->readFromCalls;

        $schema = $extractor->inferSchema(infer_schema()->allStrings())->schema();

        static::assertGreaterThan($cold, $counting->readFromCalls);
        static::assertSame(['id', 'name', 'active'], array_keys($schema->definitions()));

        foreach ($schema->definitions() as $definition) {
            static::assertSame('string', $definition->type()->toString());
            static::assertTrue($definition->isNullable());
        }
    }

    public function test_without_header_generates_the_column_names(): void
    {
        $schema = from_csv(CSVFixtureContext::path('file_with_empty_columns.csv'))->withHeader(false)->schema();

        static::assertSame(['e00', 'e01', 'e02'], array_keys($schema->definitions()));

        // the header line is data now, so it is type evidence and every column floors to string
        foreach ($schema->definitions() as $definition) {
            static::assertSame('string', $definition->type()->toString());
        }
    }

    public function test_metadata_and_partition_setters_keep_the_inferred_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('metadata_column_collision.csv'), filesystem: $counting);

        $extractor->schema();
        $cold = $counting->readFromCalls;

        $schema = $extractor->withMetadataColumns(true)->schema();

        static::assertSame($cold, $counting->readFromCalls);
        static::assertCount(1, array_filter(
            array_keys($schema->definitions()),
            static fn($n) => $n === '_input_file_uri',
        ));

        // Ruling B's mirror arm: turning metadata columns back off must leave the BODY column of that name
        static::assertArrayHasKey('_input_file_uri', $extractor->withMetadataColumns(false)->schema()->definitions());
        static::assertSame($cold, $counting->readFromCalls);
    }

    public function test_partition_types_keep_the_inferred_schema(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('partitioned/group=*/*.csv'), filesystem: $counting);

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

    public function test_sample_size_bounds_the_inferred_schema(): void
    {
        $path = CSVFixtureContext::path('annual-enterprise-survey-2019-financial-year-provisional-csv.csv');

        static::assertSame(
            'integer',
            from_csv($path)
                ->inferSchema(infer_schema()->sampleSize(10))
                ->schema()
                ->get('Industry_code_NZSIOC')
                ->type()
                ->toString(),
        );
        static::assertSame('string', from_csv($path)->schema()->get('Industry_code_NZSIOC')->type()->toString());
    }

    public function test_a_value_past_the_sample_is_reported(): void
    {
        $this->expectException(SchemaMismatchException::class);

        iterator_to_array(
            from_csv(CSVFixtureContext::path('annual-enterprise-survey-2019-financial-year-provisional-csv.csv'))
                ->inferSchema(infer_schema()->sampleSize(10))
                ->extract(flow_context(config())),
        );
    }

    public function test_files_to_sniff_bounds_the_sources_opened(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        from_csv(CSVFixtureContext::path('partitioned/group=*/*.csv'), filesystem: $counting)
            ->inferSchema(infer_schema()->filesToSniff(1))
            ->schema();

        static::assertSame(2, $counting->readFromCalls, 'header() + the single sniffed source');
    }

    public function test_columns_that_diverge_from_the_inferred_schema_are_rejected(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/unexpected \[label\].+missing \[name\]/');

        iterator_to_array(
            from_csv(CSVFixtureContext::path('columns_diverge/*.csv'))
                ->inferSchema(infer_schema()->filesToSniff(1))
                ->extract(flow_context(config())),
        );
    }

    /**
     * unionByName() disables the column-set check entirely (08c's two-resolver split), so a glob whose files
     * carry different columns reads as one wider schema instead of throwing.
     */
    public function test_union_by_name_reads_diverging_columns_as_one_wider_schema(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('columns_diverge/*.csv'))
            ->inferSchema(infer_schema()->unionByName());

        static::assertSame(['id', 'name', 'label'], array_keys($extractor->schema()->definitions()));

        $rows = [];

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            foreach ($batch as $row) {
                $rows[] = $row->toArray();
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'name' => 'a', 'label' => null],
                ['id' => 2, 'name' => 'b', 'label' => null],
                ['id' => 3, 'name' => null, 'label' => 'c'],
            ],
            $rows,
        );
    }

    public function test_the_divergence_message_names_both_sources(): void
    {
        try {
            iterator_to_array(
                from_csv(CSVFixtureContext::path('columns_diverge/*.csv'))
                    ->inferSchema(infer_schema()->filesToSniff(1))
                    ->extract(flow_context(config())),
            );
            static::fail('a divergent column set must throw');
        } catch (InferredSchemaException $e) {
            static::assertStringContainsString('b.csv', $e->getMessage());
            static::assertStringContainsString('a.csv', $e->getMessage());
            static::assertStringContainsString('20480 rows', $e->getMessage());
            static::assertStringContainsString('1 sources', $e->getMessage());
        }
    }

    public function test_a_header_only_file_with_a_divergent_header_is_rejected(): void
    {
        try {
            iterator_to_array(
                from_csv(CSVFixtureContext::path('columns_diverge_header_only/*.csv'))
                    ->inferSchema(infer_schema()->filesToSniff(1))
                    ->extract(flow_context(config())),
            );
            static::fail('a divergent header-only source must throw');
        } catch (InferredSchemaException $e) {
            static::assertMatchesRegularExpression('/unexpected \[label\].+missing \[name\]/', $e->getMessage());
            // the zero-row arm is the SECOND columnsDiverge() call site - it pins its source label too
            static::assertStringContainsString('b.csv', $e->getMessage());
            static::assertStringContainsString('a.csv', $e->getMessage());
        }
    }

    public function test_a_header_only_file_infers_every_column_as_string(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('header_only.csv'));

        static::assertSame('string', $extractor->schema()->get('id')->type()->toString());
        static::assertSame('string', $extractor->schema()->get('name')->type()->toString());
        static::assertSame([], iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_an_empty_file_infers_an_empty_schema(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('empty.csv'));

        static::assertSame([], $extractor->schema()->definitions());
        static::assertSame([], iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_a_glob_of_only_empty_files_infers_an_empty_schema(): void
    {
        static::assertSame([], from_csv(CSVFixtureContext::path('two_empty/*.csv'))->schema()->definitions());
    }

    public function test_an_empty_file_in_a_glob_is_skipped(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('glob_with_empty/*.csv'));

        static::assertSame('integer', $extractor->schema()->get('id')->type()->toString());
        static::assertSame('string', $extractor->schema()->get('name')->type()->toString());
        static::assertCount(1, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    #[TestWith(['empty_then_header_only'])]
    #[TestWith(['header_only_then_empty'])]
    public function test_a_glob_of_an_empty_and_a_header_only_file_infers_the_header_columns(string $dir): void
    {
        $extractor = from_csv(CSVFixtureContext::path($dir . '/*.csv'));

        static::assertSame(['id', 'name'], array_keys($extractor->schema()->definitions()));
        static::assertSame([], iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_empty_cells_are_not_type_evidence(): void
    {
        $schema = from_csv(CSVFixtureContext::path('file_with_empty_columns.csv'))->schema();

        static::assertSame('integer', $schema->get('id')->type()->toString());
        static::assertSame('string', $schema->get('name')->type()->toString());
        static::assertSame('boolean', $schema->get('active')->type()->toString());
    }

    public function test_without_empty_to_null_empty_cells_are_string_evidence(): void
    {
        $schema = from_csv(CSVFixtureContext::path('file_with_empty_columns.csv'))->withEmptyToNull(false)->schema();

        static::assertSame(['id', 'name', 'active'], array_keys($schema->definitions()));

        foreach ($schema->definitions() as $definition) {
            static::assertSame('string', $definition->type()->toString());
        }
    }

    public function test_partition_columns_are_not_typed_by_the_sample(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('partitioned/group=*/*.csv'));

        static::assertSame('string', $extractor->schema()->get('group')->type()->toString());

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertSame('1', $rows->first()->toArray()['group']);

            break;
        }
    }

    #[TestWith(['limit'])]
    #[TestWith(['stop'])]
    #[TestWith(['hydrator-throws'])]
    public function test_a_stream_is_closed_when_the_read_stops_early(string $mode): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        if ($mode === 'hydrator-throws') {
            $extractor = from_csv(
                CSVFixtureContext::path('annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
                filesystem: $counting,
            )->inferSchema(infer_schema()->sampleSize(10));

            try {
                iterator_to_array($extractor->extract(flow_context(config())));
                static::fail('a value past the sample must stop the read');
            } catch (SchemaMismatchException) {
            }
        } else {
            $extractor = from_csv(CSVFixtureContext::path('orders_flow.csv'), filesystem: $counting);

            if ($mode === 'limit') {
                $extractor->changeLimit(2);

                iterator_to_array($extractor->extract(flow_context(config())));
            } else {
                $rows = $extractor->extract(flow_context(config()));
                $rows->current();
                $rows->send(Signal::STOP);
            }
        }

        static::assertGreaterThan(0, $counting->readFromCalls);
        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            from_csv(
                __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
                schema: schema(str_schema('name')),
            )->schema(),
        );
    }

    /**
     * Pins the half that is NOT closed: with no declared schema there is no Schema::add() to
     * refuse, and the row-value write still clobbers the source column. The value-write sites are
     * deliberately untouched in this phase.
     */
    public function test_without_a_declared_schema_the_metadata_column_still_overwrites_silently(): void
    {
        $rows = df()
            ->read(from_csv(__DIR__ . '/../Fixtures/metadata_column_collision.csv')->withMetadataColumns(true))
            ->fetch();

        static::assertStringEndsWith(
            'metadata_column_collision.csv',
            type_string()->assert($rows->first()->get('_input_file_uri')),
        );
    }

    public function test_bom_removal_utf16_be(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf16be_bom.csv'))
            ->withMetadataColumns(true);
        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf16be_bom.csv', "\xFE\xFF"));

        static::assertSame(
            [
                [
                    [
                        'id' => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf16_le(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf16le_bom.csv'))
            ->withMetadataColumns(true);
        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf16le_bom.csv', "\xFF\xFE"));

        static::assertSame(
            [
                [
                    [
                        'id' => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf32_be(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf32be_bom.csv'))
            ->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf32be_bom.csv', "\x00\x00\xFE\xFF"));

        static::assertSame(
            [
                [
                    [
                        'id' => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf32_le(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf32le_bom.csv'))
            ->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf32le_bom.csv', "\xFF\xFE\x00\x00"));

        static::assertSame(
            [
                [
                    [
                        'id' => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf8(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf8_bom.csv'))->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf8_bom.csv', "\xEF\xBB\xBF"));

        static::assertSame(
            [
                [
                    [
                        'id' => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_extract_does_not_mutate_metadata_of_user_provided_schema(): void
    {
        $schema = schema(int_schema('id', metadata: schema_metadata(['primary_key' => true])), str_schema('value'));

        $before = $schema->normalize();

        $extractor = from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: $schema)->withMetadataColumns(
            true,
        );

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertSame($before, $schema->normalize());
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(int_schema('id'), str_schema('value'));

        $extractor = from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: $schema)->withMetadataColumns(
            true,
        );

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_extracting_csv_empty_columns_as_empty_strings(): void
    {
        $extractor = from_csv(
            $path = path_real(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'),
            empty_to_null: false,
        )->withMetadataColumns(true);

        static::assertSame(
            [
                [
                    [
                        'id' => '',
                        'name' => '',
                        'active' => 'false',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
                [
                    [
                        'id' => '1',
                        'name' => 'Norbert',
                        'active' => '',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_extracting_csv_empty_columns_as_null(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/file_with_empty_columns.csv');

        static::assertSame(
            [
                [
                    [
                        'id' => null,
                        'name' => null,
                        'active' => false,
                    ],
                ],
                [
                    [
                        'id' => 1,
                        'name' => 'Norbert',
                        'active' => null,
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }

    public function test_extracting_csv_empty_headers(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/file_with_empty_headers.csv');

        static::assertSame(
            [
                [
                    ['e00' => null, 'name' => null, 'active' => false],
                ],
                [
                    ['e00' => 1, 'name' => 'Norbert', 'active' => null],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }

    public function test_extracting_csv_files_with_header(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()->read(from_csv($path))->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'Year',
                    'Industry_aggregation_NZSIOC',
                    'Industry_code_NZSIOC',
                    'Industry_name_NZSIOC',
                    'Units',
                    'Variable_code',
                    'Variable_name',
                    'Variable_category',
                    'Value',
                    'Industry_code_ANZSIC06',
                ],
                array_keys($row->toArray()),
            );
        }

        static::assertSame(998, $rows->count());
    }

    public function test_extracting_csv_files_with_schema(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()->read(from_csv($path, schema: $schema = df()->read(from_csv($path))->schema()))->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'Year',
                    'Industry_aggregation_NZSIOC',
                    'Industry_code_NZSIOC',
                    'Industry_name_NZSIOC',
                    'Units',
                    'Variable_code',
                    'Variable_name',
                    'Variable_category',
                    'Value',
                    'Industry_code_ANZSIC06',
                ],
                array_keys($row->toArray()),
            );
        }

        static::assertSame(998, $rows->count());
        static::assertEquals($schema, $rows->schema());

        static::assertSame(<<<'SCHEMA'
            schema
            |-- Year: ?integer
            |-- Industry_aggregation_NZSIOC: ?string
            |-- Industry_code_NZSIOC: ?string
            |-- Industry_name_NZSIOC: ?string
            |-- Units: ?string
            |-- Variable_code: ?string
            |-- Variable_name: ?string
            |-- Variable_category: ?string
            |-- Value: ?string
            |-- Industry_code_ANZSIC06: ?string

            SCHEMA, schema_to_ascii($rows->schema()));
    }

    public function test_extracting_csv_files_without_header(): void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            false,
        );

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    array_keys($row->toArray()),
                );
            }
            $total += $rows->count();
        }

        static::assertSame(999, $total);
    }

    public function test_extracting_csv_with_corrupted_row(): void
    {
        $rows = df()->extract(from_csv(__DIR__ . '/../Fixtures/corrupted_row.csv'))->fetch();

        static::assertSame(3, $rows->count());
    }

    public function test_extracting_csv_with_more_columns_than_headers(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/more_columns_than_headers.csv');

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(['id', 'name'], array_keys($row->toArray()));
            }
            $total += $rows->count();
        }

        static::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_headers_than_columns(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/more_headers_than_columns.csv'));

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(['id', 'name', 'active'], array_keys($row->toArray()));
            }
            $total += $rows->count();
        }

        static::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_splits_rows(): void
    {
        static::assertCount(
            1,
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv'))
                ->fetch()
                ->toArray(),
            'Long line was broken down into two rows.',
        );
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_with_increased_read_in_line_option(): void
    {
        static::assertCount(
            1,
            df()
                ->read(from_csv(
                    __DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv',
                    characters_read_in_line: 2000,
                ))
                ->fetch()
                ->toArray(),
            'Long line was read as one row.',
        );
    }

    public function test_extracting_csv_with_multiline_strings(): void
    {
        if ($this->isWindows()) {
            static::markTestSkipped('This test is failing on windows due to different new line characters.');
        }

        $extractor = from_csv(__DIR__ . '/../Fixtures/multiline_strings.csv');

        $rows = df()->read($extractor)->fetch();

        static::assertSame(1, $rows->count());

        $row = $rows->first();
        static::assertSame('ABBA', $row->get('artist'));
        static::assertSame("Ahe's My Kind Of Girl", $row->get('song'));
        static::assertSame('/a/abba/ahes+my+kind+of+girl_20598417.html', $row->get('link'));

        $expectedText = "Look at her face, it's a wonderful face  \nAnd it means something special to me  \nLook at the way that she smiles when she sees me  \nHow lucky can one fellow be?  \n  \nShe's just my kind of girl, she makes me feel fine  \nWho could ever believe that she could be mine?  \nShe's just my kind of girl, without her I'm blue  \nAnd if she ever leaves me what could I do, what could I do?  \n  \nAnd when we go for a walk in the park  \nAnd she holds me and squeezes my hand  \nWe'll go on walking for hours and talking  \nAbout all the things that we plan  \n  \nShe's just my kind of girl, she makes me feel fine  \nWho could ever believe that she could be mine?  \nShe's just my kind of girl, without her I'm blue  \nAnd if she ever leaves me what could I do, what could I do?\n\n";

        static::assertSame($expectedText, $row->get('text'));
    }

    public function test_limit(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        static::assertCount(2, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    /**
     * p3's divergence: the fixture's own header is `group,id,value`, so before D4 extract() emitted
     * `group` first while schema() never mentioned it at all. Both now emit the partition block last.
     */
    public function test_schema_and_extract_agree_on_partition_columns(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->withSchema(schema(
            int_schema('group'),
            int_schema('id'),
            str_schema('value'),
        ));

        static::assertSame(
            ['id', 'value', 'group'],
            array_map(static fn(Row\Reference $ref): string => $ref->name(), $extractor->schema()->references()->all()),
        );

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
        }
    }

    public function test_schema_lists_the_path_once_per_instance(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->withSchema(schema(
            int_schema('id'),
            str_schema('value'),
        ));

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(
            ['id', 'value', 'group'],
            array_map(static fn(Row\Reference $ref): string => $ref->name(), $extractor->schema()->references()->all()),
        );
    }

    public function test_loading_data_from_all_partitions(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'group' => '1'],
                ['id' => 2, 'value' => 'b', 'group' => '1'],
                ['id' => 3, 'value' => 'c', 'group' => '1'],
                ['id' => 4, 'value' => 'd', 'group' => '1'],
                ['id' => 5, 'value' => 'e', 'group' => '2'],
                ['id' => 6, 'value' => 'f', 'group' => '2'],
                ['id' => 7, 'value' => 'g', 'group' => '2'],
                ['id' => 8, 'value' => 'h', 'group' => '2'],
            ],
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy([ref('id')])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['id' => 2, 'value' => 'b', 'date' => null],
            ],
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: schema(
                    int_schema('id'),
                    str_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_without_bom_removal_utf8(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf8_bom.csv'))->withMetadataColumns(true);

        $extractor = $extractor->withBOMRemoval(false);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf8_bom.csv', "\xEF\xBB\xBF"));

        static::assertSame(
            [
                [
                    [
                        "\xEF\xBB\xBFid" => 2,
                        'name' => 'asd',
                        'reference' => 144,
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_csv(CSVFixtureContext::path('two_rows.csv'))->isRepeatable());
    }

    private function ensureBOMExists(string $path, string $BOM): bool
    {
        $handle = fopen($path, 'rb');

        if ($handle === false) {
            throw new RuntimeException('Failed to open file: ' . $path);
        }

        $bomLength = strlen($BOM);

        if ($bomLength === 0) {
            fclose($handle);

            return true;
        }

        $contents = fread($handle, $bomLength);
        fclose($handle);

        return $contents === $BOM;
    }
}
