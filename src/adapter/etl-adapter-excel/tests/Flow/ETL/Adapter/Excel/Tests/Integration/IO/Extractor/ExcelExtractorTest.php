<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\IO\Extractor;

use Closure;
use DateTimeImmutable;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelExtractor;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReader;
use Flow\ETL\Adapter\Excel\Tests\Context\CraftedWorkbook;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\SplitCoverage;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\KeepPaths;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\UnsizedFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\is_valid_excel_sheet_name;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_datetime;
use function iterator_to_array;
use function max;

final class ExcelExtractorTest extends FlowTestCase
{
    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../../../Fixtures/fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../../../Fixtures/fixture.xlsx'];
        yield 'alike ods' => [__DIR__ . '/../../../Fixtures/fixture_as_ods'];
        yield 'alike xlsx' => [__DIR__ . '/../../../Fixtures/fixture_as_xlsx'];
    }

    /**
     * @return iterable<string, array<string>>
     */
    public static function provide_nullable_fixtures(): iterable
    {
        yield 'ods' => [__DIR__ . '/../../../Fixtures/nullable_fixture.ods'];
        yield 'xlsx' => [__DIR__ . '/../../../Fixtures/nullable_fixture.xlsx'];
    }

    public function test_a_row_with_more_cells_than_the_header_is_refused(): void
    {
        $file = CraftedWorkbook::with(CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="inlineStr"><is><t>a</t></is></c><c r="B1" t="inlineStr"><is><t>b</t></is></c><c r="C1" t="inlineStr"><is><t>c</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>1</t></is></c><c r="B2" t="inlineStr"><is><t>2</t></is></c><c r="C2" t="inlineStr"><is><t>3</t></is></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>4</t></is></c><c r="B3" t="inlineStr"><is><t>5</t></is></c><c r="C3" t="inlineStr"><is><t>6</t></is></c><c r="D3" t="inlineStr"><is><t>7</t></is></c><c r="E3" t="inlineStr"><is><t>8</t></is></c></row>',
        ));

        try {
            ExtractedRows::of(from_excel($file));
            static::fail('Expected the corrupted record to be refused');
        } catch (CorruptedRecordException $e) {
            static::assertSame([path_real($file)->uri(), 3, null, ['4', '5', '6', '7', '8']], [
                $e->uri,
                $e->offset,
                $e->lineNumber,
                $e->fields,
            ]);
        }
    }

    public function test_a_corrupted_row_after_skipped_empty_rows_reports_its_sheet_row_number(): void
    {
        $file = CraftedWorkbook::with(CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="inlineStr"><is><t>a</t></is></c><c r="B1" t="inlineStr"><is><t>b</t></is></c><c r="C1" t="inlineStr"><is><t>c</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>1</t></is></c><c r="B2" t="inlineStr"><is><t>2</t></is></c><c r="C2" t="inlineStr"><is><t>3</t></is></c></row><row r="5"><c r="A5" t="inlineStr"><is><t>4</t></is></c><c r="B5" t="inlineStr"><is><t>5</t></is></c><c r="C5" t="inlineStr"><is><t>6</t></is></c><c r="D5" t="inlineStr"><is><t>7</t></is></c></row>',
        ));

        try {
            ExtractedRows::of(from_excel($file));
            static::fail('Expected the corrupted record to be refused');
        } catch (CorruptedRecordException $e) {
            static::assertSame([5, ['4', '5', '6', '7']], [$e->offset, $e->fields]);
        }
    }

    public function test_a_declared_column_the_header_lacks_is_a_schema_error(): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage('"x"');

        ExtractedRows::of(
            from_excel(CraftedWorkbook::with(CraftedWorkbook::sheet(
                '<row r="1"><c r="A1" t="inlineStr"><is><t>a</t></is></c><c r="B1" t="inlineStr"><is><t>b</t></is></c><c r="C1" t="inlineStr"><is><t>c</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>1</t></is></c><c r="B2" t="inlineStr"><is><t>2</t></is></c><c r="C2" t="inlineStr"><is><t>3</t></is></c></row>',
            )))
                ->withSchema(schema(str_schema('a'), str_schema('b'), str_schema('x', true))),
        );
    }

    public function test_an_empty_glob_refuses_to_infer_a_schema(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_excel() has nothing to infer a schema from: no file matching "'
            . path_real(ExcelFixtureContext::file('*.no-match'))->uri()
            . '" carries a column. Declare the schema with ->withSchema(...).',
        );

        from_excel(ExcelFixtureContext::file('*.no-match'))->schema();
    }

    public function test_an_empty_glob_with_metadata_columns_refuses_to_infer_a_schema(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_excel() has nothing to infer a schema from: no file matching "'
            . path_real(ExcelFixtureContext::file('*.no-match'))->uri()
            . '" carries a column. Declare the schema with ->withSchema(...).',
        );

        from_excel(ExcelFixtureContext::file('*.no-match'))->withMetadataColumns(true)->schema();
    }

    public function test_an_empty_glob_with_a_declared_schema_answers_it_and_extracts_nothing(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('*.no-match'))->withSchema(schema(int_schema('id')));

        static::assertEquals(schema(int_schema('id')), $extractor->schema());
        static::assertSame(
            [],
            iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false),
        );
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertNull($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_empty_cells(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName)->withConvertEmptyToNull(false))->fetch()->toArray();

        static::assertCount(10, $rows);
        static::assertEmpty($rows[9]['email']);
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_limit(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->withBatchSize(1);

        $rows = df()->read($extractor)->limit(5)->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_offset(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName))->offset(3)->fetch()->toArray();

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

        $rows = df()->read($extractor)->offset(4)->fetch()->toArray();

        static::assertCount(7, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_with_selected_sheet_name(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName)->withSheetName('Sheet2'))->fetch()->toArray();

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

        df()->read(from_excel($fixtureName)->withSheetName('unknown'))->fetch()->toArray();
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_file_without_header(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName)->withHeader(false))->fetch()->toArray();

        static::assertCount(11, $rows);

        foreach ($rows as $row) {
            static::assertSame(['e00', 'e01', 'e02'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_nullable_fixtures')]
    public function test_extract_excel_nullable_file(string $fixtureName): void
    {
        $rows = df()->read(from_excel($fixtureName))->fetch()->toArray();

        static::assertCount(5, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'name', 'email'], array_keys($row));
            static::assertCount(3, $row);
        }
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_refuses_a_declared_nullable_column_the_sheet_lacks(string $fixtureName): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage('Columns "missing" declared by the schema are not in the header of');

        df()
            ->read(from_excel($fixtureName)->withSchema(schema(
                int_schema('id'),
                string_schema('name'),
                string_schema('email', nullable: true),
                string_schema('missing', nullable: true),
            )))
            ->fetch();
    }

    /**
     * b57: the sheet has no such column, so every row would carry a null under a NOT NULL
     * declaration. Declare the column nullable if that is what the data is.
     */
    #[DataProvider('provide_fixtures')]
    public function test_extract_excel_refuses_a_declared_column_the_sheet_lacks(string $fixtureName): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage('Columns "missing" declared by the schema are not in the header of');

        df()
            ->read(from_excel($fixtureName)->withSchema(schema(
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
            ->read(from_excel(__DIR__ . '/../../../Fixtures/fixture.ods')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);
        static::assertSame(['id', 'name', 'email'], array_keys($rows[0]));
    }

    public function test_extract_with_explicit_xlsx_reader(): void
    {
        $rows = df()
            ->read(from_excel(__DIR__ . '/../../../Fixtures/fixture.xlsx')->withReader(ExcelReader::XLSX))
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
            ->read(from_excel(__DIR__ . '/../../../Fixtures/empty_file'))
            ->fetch()
            ->toArray();
    }

    public function test_extract_with_wrongly_selected_reader(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Failed to open file: Could not open');

        df()
            ->read(from_excel(__DIR__ . '/../../../Fixtures/fixture.xlsx')->withReader(ExcelReader::ODS))
            ->fetch()
            ->toArray();
    }

    public function test_is_valid_excel_sheet_name_function(): void
    {
        $result = df()
            ->read(from_rows(array_to_rows([
                ['sheet' => 'ValidSheet'],
                ['sheet' => 'Invalid/Sheet'],
                ['sheet' => 'Sheet*Name'],
                ['sheet' => 'This is a very long sheet name that exceeds the 31 character limit'],
                ['sheet' => 'Normal'],
            ], schema(string_schema('sheet')))))
            ->withColumn('is_valid', is_valid_excel_sheet_name(ref('sheet')))
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

        $extractor = from_excel(__DIR__ . '/../../../Fixtures/cross_stream/*/*.xlsx')
            ->withSchema($schema)
            ->withMetadataColumns(true);

        df(config_builder())->read($extractor)->run();
        df(config_builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_loading_data_from_all_partitions(): void
    {
        df()
            ->read(from_excel(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx'))
            ->forEach(function (Rows $rows): void {
                // the partition column comes back as a column, discovered from the path
                $this->assertContains('group', $rows->schema()->names());
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
                ->read(from_excel(__DIR__ . '/../../../Fixtures/cross_stream/*/*.xlsx')->withSchema(schema(
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

        foreach (FlowTestCase::extracted(
            $extractor->withBatchSize(2),
            flow_context()->backend(),
            new Criteria(),
        ) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            $sizes[] = $rows->count();

            foreach ($rows->toArray() as $row) {
                $emails[] = $row['email'];
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
            df()->read($extractor)->fetch()->toArray()[2]['d'],
        );
    }

    public function test_a_later_file_with_a_different_header_is_fine_when_a_schema_is_declared(): void
    {
        static::assertCount(
            2,
            df()
                ->read(
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

        static::assertSame(['id', 'name', 'email'], $extractor->schema()->names());
        static::assertCount(10, df()->read($extractor)->fetch()->toArray());
    }

    public function test_a_later_file_with_a_different_header_is_read_as_one_wider_schema_under_union_by_name(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx'))
            ->inferSchema(infer_schema()->unionByName());

        static::assertSame(['id', 'name', 'zip'], $extractor->schema()->names());
        static::assertSame(
            [
                ['id' => 1, 'name' => 'one', 'zip' => null],
                ['id' => 2, 'name' => 'two', 'zip' => '00-001'],
            ],
            df()->read($extractor)->fetch()->toArray(),
        );
    }

    public function test_a_later_file_with_a_different_header_throws(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches(
            '#do not match the schema inferred from \S+/diverging_header/a\.xlsx: unexpected \[zip\], missing \[\]\. '
            . 'The sample was at most 20480 rows over at most 10 sources\.#',
        );

        df()->read(from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx')))->fetch();
    }

    public function test_a_value_past_the_sample_that_does_not_fit_fails_naming_column_and_row(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('int_then_str.xlsx'))
            ->inferSchema(infer_schema()->sampleSize(2));

        static::assertEquals(schema(int_schema('v', nullable: true)), $extractor->schema());

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('column "v"');

        df()->read($extractor)->fetch();
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

    #[TestWith(['header_only.xlsx', ['id', 'name']])]
    #[TestWith(['header_only.ods', ['id', 'name']])]
    public function test_header_only_sheets(string $fixture, array $names): void
    {
        $extractor = from_excel(ExcelFixtureContext::file($fixture));

        static::assertSame($names, $extractor->schema()->names());
        static::assertCount(0, df()->read($extractor)->fetch()->toArray());
    }

    #[TestWith(['empty_sheet.xlsx'])]
    #[TestWith(['empty_sheet.ods'])]
    public function test_an_empty_sheet_refuses_to_infer_a_schema_and_extracts_nothing(string $fixture): void
    {
        $extractor = from_excel(ExcelFixtureContext::file($fixture));

        static::assertSame(
            [],
            iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false),
        );

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_excel() has nothing to infer a schema from: no file matching "'
            . path_real(ExcelFixtureContext::file($fixture))->uri()
            . '" carries a column. Declare the schema with ->withSchema(...).',
        );

        $extractor->schema();
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

        df()
            ->read(from_excel(ExcelFixtureContext::file('orders_flow.xlsx')))
            ->limit(1)
            ->run();

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_limit_pays_the_sample_but_yields_only_the_limit(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('orders_1k.xlsx'));
        $extractor->withBatchSize(1);

        static::assertCount(5, df()->read($extractor)->limit(5)->fetch()->toArray());
        static::assertNotEmpty($extractor->schema()->names());
    }

    public function test_mixed_formats_in_one_glob(): void
    {
        static::assertCount(
            20,
            df()
                ->read(from_excel(ExcelFixtureContext::file('mixed_formats/*')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_partitioned_glob_infers_from_the_first_file_only(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('partitioned/group=*/*.xlsx'))
            ->inferSchema(infer_schema()->filesToSniff(1));

        $count = 0;

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertContains('group', $rows->schema()->names());

            $count += $rows->count();
        }

        static::assertSame(8, $count);
    }

    #[DataProvider('provide_fixtures')]
    public function test_schema_then_extract_on_the_same_instance(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);
        $extractor->schema();

        static::assertCount(10, df()->read($extractor)->fetch()->toArray());
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

        $generator = FlowTestCase::extracted(
            from_excel(ExcelFixtureContext::path('orders_flow.xlsx')),
            flow_context(config())->backend(),
            new Criteria(),
        );
        $generator->current();
        $generator->send(Signal::STOP);
        unset($generator);

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    #[DataProvider('provide_fixtures')]
    public function test_extract_twice_on_the_same_instance(string $fixtureName): void
    {
        $extractor = from_excel($fixtureName);

        $generator = FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria());
        $generator->current();
        $generator->next();
        $generator->send(Signal::STOP);
        unset($generator);

        static::assertCount(10, df()->read($extractor)->fetch()->toArray());
    }

    /**
     * @return Generator<string, array{Closure(ExcelExtractor): void, int}>
     */
    public static function provide_setters_and_the_reads_they_cost(): Generator
    {
        yield 'withHeader resets the memo' => [static fn(ExcelExtractor $e) => $e->withHeader(false), 1];
        yield 'withSheetName resets the memo' => [static fn(ExcelExtractor $e) => $e->withSheetName('Sheet2'), 1];
        yield 'withConvertEmptyToNull resets the memo' => [
            static fn(ExcelExtractor $e) => $e->withConvertEmptyToNull(false),
            1,
        ];
        yield 'inferSchema resets the memo' => [
            static fn(ExcelExtractor $e) => $e->inferSchema(infer_schema()->sampleSize(1)),
            1,
        ];
        yield 'withSchema keeps it' => [
            static fn(ExcelExtractor $e) => $e->withSchema(schema(str_schema('id'))),
            0,
        ];
        yield 'withMetadataColumns keeps it' => [
            static fn(ExcelExtractor $e) => $e->withMetadataColumns(true),
            0,
        ];
    }

    #[DataProvider('provide_setters_and_the_reads_they_cost')]
    public function test_each_shape_setter_resets_the_inferred_schema_memo(Closure $setter, int $reads): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(ExcelFixtureContext::file('sniff/a'), $filesystem);
        $extractor->schema();
        $opened = $filesystem->readFromCalls;

        $setter($extractor);
        $extractor->schema();

        // a re-sample opens the workbook for its signature sniff; a memo hit opens nothing
        static::assertSame($reads, $filesystem->readFromCalls - $opened);
    }

    public function test_with_reader_resets_the_inferred_schema_memo(): void
    {
        $extractor = from_excel(ExcelFixtureContext::file('sniff/a'));
        $extractor->schema();
        $extractor->withReader(ExcelReader::ODS);

        // the reader is pinned, so the re-sample skips the signature sniff; an ods reader cannot open this xlsx
        $this->expectException(InvalidArgumentException::class);

        $extractor->schema();
    }

    public function test_infer_schema_knobs_reach_the_sample(): void
    {
        $schema = from_excel(ExcelFixtureContext::file('fixture.xlsx'))
            ->inferSchema(infer_schema()->allStrings())
            ->schema();

        static::assertSame(['id', 'name', 'email'], $schema->names());

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
        $generator = FlowTestCase::extracted(
            from_excel(path_real(__DIR__ . '/../../../Fixtures/fixture.xlsx'))->withBatchSize(1),
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

    public function test_signal_stop_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $generator = FlowTestCase::extracted(
            from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx'))->withBatchSize(10),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_limit_reached_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        static::assertCount(2, ExtractedRows::of(
            from_excel(ExcelFixtureContext::file('cross_stream/*/*.xlsx'))->withBatchSize(10),
            criteria: new Criteria(2),
        ));
    }

    /**
     * @return Generator<string, array{Closure(ExcelExtractor): mixed}>
     */
    public static function sample_dropping_setters(): Generator
    {
        yield 'inferSchema' => [static fn(ExcelExtractor $e): mixed => $e->inferSchema(infer_schema())];
        yield 'withConvertEmptyToNull' => [static fn(ExcelExtractor $e): mixed => $e->withConvertEmptyToNull(false)];
        yield 'withHeader' => [static fn(ExcelExtractor $e): mixed => $e->withHeader(true)];
        yield 'withReader' => [static fn(ExcelExtractor $e): mixed => $e->withReader(ExcelReader::XLSX)];
        yield 'withSheetName' => [static fn(ExcelExtractor $e): mixed => $e->withSheetName('Sheet1')];
    }

    #[DataProvider('sample_dropping_setters')]
    public function test_a_changed_read_option_drops_the_sampled_rows(Closure $setter): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(10), $extractor->statistics()->rows);

        $setter($extractor);

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
    }

    public function test_a_declared_schema_declares_unknown_rows(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'))->withSchema(schema(int_schema('id')));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
    }

    public function test_a_member_without_a_size_makes_the_size_unknown(): void
    {
        static::assertEquals(
            Cardinality::unknown(),
            from_excel(
                ExcelFixtureContext::path('sniff/*'),
                new UnsizedFilesystem(native_local_filesystem(), [ExcelFixtureContext::path('sniff/b')->uri()]),
            )->statistics()->size,
        );
    }

    public function test_a_sample_that_read_every_sheet_declares_exact_rows(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/*'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(20), $extractor->statistics()->rows);
    }

    public function test_a_sample_that_stopped_early_declares_unknown_rows(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/*'))->inferSchema(infer_schema()->sampleSize(3));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
    }

    public function test_extract_samples_like_schema_does(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'));

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false);

        static::assertEquals(Cardinality::exact(10), $extractor->statistics()->rows);
    }

    public function test_it_declares_the_listed_byte_total_exactly(): void
    {
        static::assertEquals(
            Cardinality::exact(17_504),
            from_excel(ExcelFixtureContext::path('sniff/*'))->statistics()->size,
        );
    }

    public function test_statistics_before_schema_declares_unknown_rows(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'));

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);

        $extractor->schema();

        static::assertEquals(Cardinality::exact(10), $extractor->statistics()->rows);
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'), $filesystem);
        $extractor->schema();

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_the_listing_is_read_once(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $extractor = from_excel(ExcelFixtureContext::path('sniff/a'), $filesystem);

        $extractor->statistics();
        $extractor->statistics();

        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_a_filtered_extract_infers_over_every_listed_file(): void
    {
        $filtered = from_excel(ExcelFixtureContext::path('sniff/*'));
        $whole = from_excel(ExcelFixtureContext::path('sniff/*'));

        iterator_to_array(FlowTestCase::extracted(
            $filtered,
            flow_context()->backend(),
            new Criteria(pathFilter: new KeepPaths([
                ExcelFixtureContext::path('sniff/a'),
            ])),
        ), false);
        $whole->schema();

        static::assertEquals($whole->statistics()->rows, $filtered->statistics()->rows);
    }

    public function test_date_cell_in_a_zoned_column_is_converted(): void
    {
        static::assertSame(
            '2024-01-03 11:30:00 Europe/Warsaw',
            type_datetime()
                ->assert(
                    df()
                        ->read(
                            from_excel(ExcelFixtureContext::file('dates_mixed.xlsx'))
                                ->withSchema(schema(datetime_schema('d', zone: 'Europe/Warsaw'))),
                        )
                        ->fetch()
                        ->column('d')
                        ->value(2),
                )
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_date_cells_read_as_utc_wall_clock_under_non_utc_default(): void
    {
        $previous = date_default_timezone_get();
        date_default_timezone_set('Asia/Tokyo');

        try {
            static::assertSame(
                '2024-01-03 10:30:00 UTC',
                type_datetime()
                    ->assert(
                        df()
                            ->read(from_excel(ExcelFixtureContext::file('dates_mixed.xlsx')))
                            ->fetch()
                            ->column('d')
                            ->value(2),
                    )
                    ->format('Y-m-d H:i:s e'),
            );
            static::assertSame(
                '2024-01-03 10:30:00 UTC',
                type_datetime()
                    ->assert(
                        df()
                            ->read(from_excel(ExcelFixtureContext::file('dates_mixed.ods')))
                            ->fetch()
                            ->column('d')
                            ->value(1),
                    )
                    ->format('Y-m-d H:i:s e'),
            );
        } finally {
            date_default_timezone_set($previous);
        }
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $backend = new SpyBackend();

        iterator_to_array(
            FlowTestCase::extracted(
                from_excel(__DIR__ . '/../../../Fixtures/fixture.xlsx'),
                flow_context(config_builder()->backend($backend)->build())->backend(),
                new Criteria(),
            ),
            false,
        );

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    public function test_one_extractor_read_twice_interleaved_gives_each_read_every_row(): void
    {
        $extractor = from_excel(ExcelFixtureContext::path('sniff/*'))->withBatchSize(1);
        $extractor->schema();
        [$first, $second] = ExtractedRows::interleaved($extractor);

        static::assertGreaterThan(0, $first->count());
        static::assertSame($first->toArray(), $second->toArray());
    }

    public function test_partition_schema_is_the_partition_block_of_the_read_schema(): void
    {
        df()
            ->read(from_array(
                [
                    ['date' => new DateTimeImmutable('2026-10-01 00:00:00 UTC'), 'text' => 'a'],
                    ['date' => new DateTimeImmutable('2026-10-02 00:00:00 UTC'), 'text' => 'b'],
                ],
                schema(datetime_schema('date'), str_schema('text')),
            ))
            ->write(
                to_excel(__DIR__ . '/var/partition_schema/file.xlsx')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('date')->writeColumns()),
            )
            ->run();

        $extractor = from_excel(__DIR__ . '/var/partition_schema/**/*.xlsx');

        static::assertEquals($extractor->schema()->keep('date'), $extractor->partitionSchema());
    }

    public function test_extract_split_stops_on_the_stop_signal(): void
    {
        $extractor = from_excel(__DIR__ . '/../../../Fixtures/orders_1k.xlsx');
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
            [2, 2, 2, 2],
            SplitCoverage::rowCountPerSplit(
                from_excel(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx'),
                flow_context(),
                1,
            ),
        );
    }

    public function test_splits_preserve_source_order(): void
    {
        static::assertEquals(
            ExtractedRows::of(from_excel(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx')),
            SplitCoverage::rowsFromSplits(
                from_excel(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx'),
                flow_context(),
                1,
            ),
        );
    }

    public function test_a_split_whose_header_diverges_from_the_inferred_schema_is_refused(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('unexpected [zip], missing []');

        SplitCoverage::rowCountPerSplit(
            from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx')),
            flow_context(),
            1,
        );
    }

    public function test_a_split_whose_header_diverges_is_read_when_a_schema_is_declared(): void
    {
        static::assertSame(
            [1, 1],
            SplitCoverage::rowCountPerSplit(
                from_excel(ExcelFixtureContext::file('diverging_header/*.xlsx'))
                    ->withSchema(schema(int_schema('id'), string_schema('name'))),
                flow_context(),
                1,
            ),
        );
    }

    public function test_a_pushed_path_filter_prunes_the_files_splits_are_cut_from(): void
    {
        $extractor = from_excel(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx');

        static::assertEquals(
            [new FileSplit($extractor->source(), 0, null)],
            $extractor->splits((new Criteria())->withPathFilter(new RejectingFilter()), 1),
        );
    }

    public function test_corrupted_records_are_routed_to_the_reject_loader(): void
    {
        $rejects = [];
        $file = CraftedWorkbook::with(CraftedWorkbook::sheet(
            '<row r="1"><c r="A1" t="inlineStr"><is><t>a</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>1</t></is></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>2</t></is></c><c r="B3" t="inlineStr"><is><t>x</t></is></c></row><row r="4"><c r="A4" t="inlineStr"><is><t>3</t></is></c></row>',
        ));

        $rows = df()->read(from_excel($file)->withCorruptedRecords(to_array($rejects)))->fetch();

        static::assertSame(['1', '3'], $rows->reduceToArray('a'));
        static::assertSame(
            [[path_real($file)->uri(), 3, null, ['2', 'x']]],
            array_map(static fn(array $reject): array => [
                $reject['uri'],
                $reject['offset'],
                $reject['line'],
                $reject['fields'],
            ], $rejects),
        );
    }
}
