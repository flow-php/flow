<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Closure;
use DateTimeImmutable;
use Flow\ETL\Adapter\Excel\ExcelExtractor;
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Config;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\is_valid_excel_sheet_name;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path_real;
use function max;

final class ExcelExtractorTest extends FlowTestCase
{
    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/fixture.xlsx'];
        yield 'alike ods' => [__DIR__ . '/../Fixtures/fixture_as_ods'];
        yield 'alike xlsx' => [__DIR__ . '/../Fixtures/fixture_as_xlsx'];
    }

    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_nullable_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../Fixtures/nullable_fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../Fixtures/nullable_fixture.xlsx'];
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertNull($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_empty_cells(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withConvertEmptyToNull(false))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertEmpty($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_limit(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withBatchSize(1)->pushLimit(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withOffset(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(7, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset_without_header(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withHeader(false);
        $extractor->withOffset(5);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(7, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_selected_sheet_name(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withSheetName('Sheet2'))->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_unknown_sheet_name(string $fixtureName): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet with name: 'unknown' not found.");

        df()->extract(from_excel($fixtureName)->withSheetName('unknown'))->fetch()->toArray();
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_without_header(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName)->withHeader(false))->fetch()->toArray();

        static::assertCount(11, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_nullable_fixtures')]
    public function test_extract_excel_nullable_file(string $fixtureName): void
    {
        $rows = df()->extract(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_puts_null_in_a_declared_nullable_column_the_sheet_lacks(string $fixtureName): void
    {
        $rows = df()
            ->extract(from_excel($fixtureName)->withSchema(schema(
                int_schema('id'),
                string_schema('name'),
                string_schema('email', nullable: true),
                string_schema('missing', nullable: true),
            )))
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            static::assertNotSame([], $row);
            static::assertNull($row['missing']);
        }
    }

    /**
     * b57: the sheet has no such column, so every row would carry a null under a NOT NULL
     * declaration. Declare the column nullable if that is what the data is.
     */
    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_refuses_a_not_null_column_the_sheet_lacks(string $fixtureName): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "missing" (row 0) declared by the schema is missing from the row');

        df()
            ->extract(from_excel($fixtureName)->withSchema(schema(
                int_schema('id'),
                string_schema('name'),
                // the fixture carries a null email in its last row; declaring it nullable leaves the
                // absent column as the only violation, which is what this test is named for
                string_schema('email', nullable: true),
                string_schema('missing'),
            )))
            ->fetch();
    }

    public function test_extract_with_explicit_ods_reader(): void
    {
        $rows = df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.ods')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]));
    }

    public function test_extract_with_explicit_xlsx_reader(): void
    {
        $rows = df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.xlsx')->withReader(ExcelReader::XLSX))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]));
    }

    public function test_extract_with_unknown_file(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported file format: n/a');

        df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/empty_file'))
            ->fetch()
            ->toArray();
    }

    public function test_extract_with_wrongly_selected_reader(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        df()
            ->extract(from_excel(__DIR__ . '/../Fixtures/fixture.xlsx')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();
    }

    public function test_is_valid_excel_sheet_name_function(): void
    {
        $result = df()
            ->read(from_rows(rows(
                schema(string_schema('sheet')),
                row(['sheet' => 'ValidSheet']),
                row(['sheet' => 'Invalid/Sheet']),
                row(['sheet' => 'Sheet*Name']),
                row(['sheet' => 'This is a very long sheet name that exceeds the 31 character limit']),
                row(['sheet' => 'Normal']),
            )))
            ->withEntry('is_valid', is_valid_excel_sheet_name(ref('sheet')))
            ->fetch()
            ->toArray();

        static::assertTrue($result[0]['is_valid']);
        static::assertFalse($result[1]['is_valid']);
        static::assertFalse($result[2]['is_valid']);
        static::assertFalse($result[3]['is_valid']);
        static::assertTrue($result[4]['is_valid']);
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(string_schema('group'), int_schema('id'), string_schema('value'));

        $extractor = from_excel(__DIR__ . '/../Fixtures/cross_stream/*/*.xlsx')
            ->withSchema($schema)
            ->withMetadataColumns(true);

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_loading_data_from_all_partitions(): void
    {
        df()->read(from_excel(__DIR__ . '/../Fixtures/partitioned/group=*/*.xlsx'))->run(function (Rows $rows): void {
            // the partition column comes back as a column, discovered from the path
            $this->assertContains('group', $rows->schema()->references()->names());
        });
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['group' => '1', 'id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['group' => '1', 'id' => 2, 'value' => 'b', 'date' => '2026-01-01'],
                ['group' => '2', 'id' => 5, 'value' => 'e', 'date' => null],
                ['group' => '2', 'id' => 6, 'value' => 'f', 'date' => null],
            ],
            df()
                ->read(from_excel(__DIR__ . '/../Fixtures/cross_stream/*/*.xlsx')->withSchema(schema(
                    string_schema('group'),
                    int_schema('id'),
                    string_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    #[DataProvider('provide_fixtures')]
    public function test_every_batch_carries_the_inferred_schema(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $emails = [];
        $sizes = [];

        foreach ($extractor->withBatchSize(2)->extract(flow_context()) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            $sizes[] = $rows->count();

            foreach ($rows as $row) {
                $emails[] = $row->get('email');
            }
        }

        static::assertCount(10, $emails);
        static::assertNull($emails[9]);
        static::assertLessThanOrEqual(2, max($sizes));
        static::assertContains(2, $sizes);
    }

    public function test_a_datetime_past_a_date_sample_is_truncated(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('dates_mixed.xlsx'))
            ->inferSchema(infer_schema()->sampleSize(2));

        static::assertEquals(schema(date_schema('d', nullable: true)), $extractor->schema());

        static::assertEquals(
            new DateTimeImmutable('2024-01-03 00:00:00'),
            df()->extract($extractor)->fetch()->toArray()[2]['d'],
        );
    }

    public function test_a_later_file_with_a_different_header_is_fine_when_a_schema_is_declared(): void
    {
        static::assertCount(
            2,
            df()
                ->extract(
                    from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx'))
                        ->withSchema(schema(int_schema('id'), string_schema('name'))),
                )
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_glob_whose_first_workbook_has_no_rows_reads_the_rest(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('empty_first/*.xlsx'));

        static::assertSame(['id', 'name', 'email'], $extractor->schema()->references()->names());
        static::assertCount(10, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_a_later_file_with_a_different_header_is_read_as_one_wider_schema_under_union_by_name(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx'))
            ->inferSchema(infer_schema()->unionByName());

        static::assertSame(['id', 'name', 'zip'], $extractor->schema()->references()->names());
        static::assertSame(
            [
                ['id' => 1, 'name' => 'one', 'zip' => null],
                ['id' => 2, 'name' => 'two', 'zip' => '00-001'],
            ],
            df()->extract($extractor)->fetch()->toArray(),
        );
    }

    public function test_a_later_file_with_a_different_header_throws(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'unexpected [zip], missing []. The sample was at most 20480 rows over at most 10 sources.',
        );

        df()->extract(from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx')))->fetch();
    }

    public function test_a_value_past_the_sample_that_does_not_fit_fails_naming_column_and_row(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('int_then_str.xlsx'))
            ->inferSchema(infer_schema()->sampleSize(2));

        static::assertEquals(schema(int_schema('v', nullable: true)), $extractor->schema());

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "v"');

        df()->extract($extractor)->fetch();
    }

    public function test_cross_stream_infers_one_schema_and_the_partition_column(): void
    {
        static::assertEquals(
            schema(
                int_schema('group', nullable: true),
                int_schema('id', nullable: true),
                string_schema('value', nullable: true),
                string_schema('date', nullable: true),
            ),
            from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx'))->schema(),
        );
        static::assertSame(
            [
                ['group' => 1, 'id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['group' => 1, 'id' => 2, 'value' => 'b', 'date' => '2026-01-01'],
                ['group' => 2, 'id' => 5, 'value' => 'e', 'date' => null],
                ['group' => 2, 'id' => 6, 'value' => 'f', 'date' => null],
            ],
            df()
                ->read(from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx')))
                ->fetch()
                ->toArray(),
        );
    }

    #[TestWith(['empty_sheet.xlsx', []])]
    #[TestWith(['empty_sheet.ods', []])]
    #[TestWith(['header_only.xlsx', ['id', 'name']])]
    #[TestWith(['header_only.ods', ['id', 'name']])]
    public function test_header_only_and_empty_sheets(string $fixture, array $names): void
    {
        $extractor = from_excel(ExcelFixtureContext::file($fixture));

        static::assertSame($names, $extractor->schema()->references()->names());
        static::assertCount(0, df()->extract($extractor)->fetch()->toArray());
    }

    /**
     * Guards the three tests below: OpenSpout only writes a shared-strings folder when it picks the file-based
     * cache, which depends on the runner's memory_limit and the fixture's unique-string count. If it ever picks
     * the in-memory strategy here, those three assert nothing at all - and pass.
     */
    public function test_the_leak_fixture_forces_the_file_based_shared_strings_cache(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sheet = ExcelFixtureContext::sheet('orders_flow.xlsx');
        $sheet->columns();

        static::assertNotSame(
            [],
            ExcelFixtureContext::leakedSharedStringsFoldersSince($before),
            'orders_flow.xlsx no longer forces OpenSpout\'s file-based shared-strings cache, so the three '
            . 'leak tests below assert nothing. Lower memory_limit (they are live at 512M and below) or give '
            . 'the fixture more unique strings.',
        );

        $sheet->close();
    }

    public function test_limit_leaves_no_shared_strings_temp_folder(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();

        $extractor = from_excel(ExcelFixtureContext::file('orders_flow.xlsx'));
        $extractor->pushLimit(1);

        df()->read($extractor)->run();

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_limit_pays_the_sample_but_yields_only_the_limit(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('orders_1k.xlsx'));
        $extractor->withBatchSize(1)->pushLimit(5);

        static::assertCount(5, df()->extract($extractor)->fetch()->toArray());
        static::assertNotEmpty($extractor->schema()->references()->names());
    }

    public function test_mixed_formats_in_one_glob(): void
    {
        static::assertCount(
            20,
            df()
                ->extract(from_excel(ExcelFixtureContext::file('mixed_formats/*')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_partitioned_glob_infers_from_the_first_file_only(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('partitioned/group=*/*.xlsx'))
            ->inferSchema(infer_schema()->filesToSniff(1));

        $count = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertContains('group', $rows->schema()->references()->names());

            $count += $rows->count();
        }

        static::assertSame(8, $count);
    }

    #[DataProvider('provide_fixtures')]
    public function test_schema_then_extract_on_the_same_instance(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->schema();

        static::assertCount(10, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_the_same_schema_on_both_hydrators(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('fixture.xlsx'));

        $default = df()
            ->extract(from_excel(ExcelFixtureContext::file('fixture.xlsx')))
            ->fetch()
            ->toArray();
        $adaptive = [];

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());

            foreach ($rows->toArray() as $row) {
                $adaptive[] = $row;
            }
        }

        static::assertCount(10, $adaptive);
        static::assertSame($default, $adaptive);
    }

    public function test_the_sample_leaves_no_shared_strings_temp_folder(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();

        from_excel(ExcelFixtureContext::file('orders_flow.xlsx'))->inferSchema(infer_schema()->sampleSize(1))->schema();

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_the_first_extract_after_the_schema_reads_on_from_its_sample(): void
    {
        // sniff/a is extension-less, so every open of it costs one readFrom
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(
            ExcelFixtureContext::path('sniff/a'),
            $filesystem,
        )->inferSchema(infer_schema()->sampleSize(1));
        $extractor->schema();

        static::assertEquals(
            ExtractedRows::of(from_excel(ExcelFixtureContext::path('sniff/a'))),
            ExtractedRows::of($extractor),
        );
        static::assertSame(1, $filesystem->readFromCalls);

        ExtractedRows::of($extractor);

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_a_changed_read_option_drops_the_sample(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(
            ExcelFixtureContext::path('sniff/a'),
            $filesystem,
        )->inferSchema(infer_schema()->sampleSize(1));
        $extractor->schema();
        $extractor->withHeader(true);

        ExtractedRows::of($extractor);

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_an_unbounded_sample_is_parsed_again(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(
            ExcelFixtureContext::path('sniff/a'),
            $filesystem,
        )->inferSchema(infer_schema()->sampleSize(-1));
        $extractor->schema();

        ExtractedRows::of($extractor);

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_signal_stop_leaves_no_shared_strings_temp_folder(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();

        $generator = from_excel(ExcelFixtureContext::path('orders_flow.xlsx'))->extract(flow_context(config()));
        $generator->current();
        $generator->send(Signal::STOP);
        unset($generator);

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_twice_on_the_same_instance(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);

        $generator = $extractor->extract(flow_context(config()));
        $generator->current();
        $generator->next();
        $generator->send(Signal::STOP);
        unset($generator);

        static::assertCount(10, df()->extract($extractor)->fetch()->toArray());
    }

    /**
     * @return Generator<string, array{Closure(ExcelExtractor): void, int}>
     */
    public static function provide_setters_and_the_listings_they_cost(): Generator
    {
        yield 'withHeader resets the memo' => [static fn(ExcelExtractor $e) => $e->withHeader(false), 3];
        yield 'withOffset resets the memo' => [static fn(ExcelExtractor $e) => $e->withOffset(2), 3];
        yield 'withSheetName resets the memo' => [static fn(ExcelExtractor $e) => $e->withSheetName('Sheet2'), 3];
        yield 'withReader resets the memo' => [
            static fn(ExcelExtractor $e) => $e->withReader(ExcelReader::XLSX),
            3,
        ];
        yield 'withConvertEmptyToNull resets the memo' => [
            static fn(ExcelExtractor $e) => $e->withConvertEmptyToNull(false),
            3,
        ];
        yield 'inferSchema resets the memo' => [
            static fn(ExcelExtractor $e) => $e->inferSchema(infer_schema()->sampleSize(1)),
            3,
        ];
        yield 'withSchema keeps it' => [
            static fn(ExcelExtractor $e) => $e->withSchema(schema(str_schema('id'))),
            2,
        ];
        yield 'withMetadataColumns keeps it' => [
            static fn(ExcelExtractor $e) => $e->withMetadataColumns(true),
            2,
        ];
    }

    #[DataProvider('provide_setters_and_the_listings_they_cost')]
    public function test_each_shape_setter_resets_the_inferred_schema_memo(Closure $setter, int $listings): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(ExcelFixtureContext::file('sniff/a'), $filesystem);

        $extractor->schema();
        $setter($extractor);
        $extractor->schema();

        // a re-sample re-lists the sources; a memo hit never reaches sourceFiles(). readFromCalls cannot
        // stand in for this - withReader() pins the format, so its re-sample skips the signature sniff.
        static::assertSame($listings, $filesystem->listCalls);
    }

    public function test_infer_schema_knobs_reach_the_sample(): void
    {
        $schema = from_excel(ExcelFixtureContext::file('fixture.xlsx'))
            ->inferSchema(infer_schema()->allStrings())
            ->schema();

        static::assertSame(['id', 'name', 'email'], $schema->references()->names());

        foreach ($schema->definitions() as $definition) {
            static::assertSame('string', $definition->type()->toString());
        }
    }

    public function test_inferred_schema_is_memoised(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(ExcelFixtureContext::file('sniff/a'), $filesystem);

        $extractor->schema();
        $extractor->schema();

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_schema_is_inferred_when_it_was_not_declared(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            from_excel(ExcelFixtureContext::file('fixture.xlsx'))->schema(),
        );
    }

    public function test_a_declared_schema_is_never_sampled_for(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        from_excel(ExcelFixtureContext::file('sniff/a'), $filesystem)->withSchema(schema(str_schema('id')))->schema();

        static::assertSame(0, $filesystem->readFromCalls);
    }

    public function test_signal_stop(): void
    {
        $generator = from_excel(path_real(__DIR__ . '/../Fixtures/fixture.xlsx'))
            ->withBatchSize(1)
            ->extract(flow_context(config()));

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
        $generator = from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx'))
            ->withBatchSize(10)
            ->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_limit_reached_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx'))->withBatchSize(10);
        $extractor->pushLimit(2);

        static::assertCount(2, ExtractedRows::of($extractor));
    }
}
