<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\IO\Extractor;

use Closure;
use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\StubSpreadsheetsResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Google\Service\Sheets;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function iterator_to_array;
use function max;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    public static function memo_keeping_setters(): iterable
    {
        yield 'withRowsPerPage' => [static fn(GoogleSheetExtractor $e): mixed => $e->withRowsPerPage(5)];
        yield 'withMetadataColumns' => [static fn(GoogleSheetExtractor $e): mixed => $e->withMetadataColumns(true)];
    }

    public static function memo_resetting_setters(): iterable
    {
        yield 'withHeader' => [static fn(GoogleSheetExtractor $e): mixed => $e->withHeader(false)];
        yield 'withMissingColumnsAsNull' => [
            static fn(GoogleSheetExtractor $e): mixed => $e->withMissingColumnsAsNull(true),
        ];
        yield 'withOptions' => [
            static fn(GoogleSheetExtractor $e): mixed => $e->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE']),
        ];
        yield 'withEmptyToNull' => [static fn(GoogleSheetExtractor $e): mixed => $e->withEmptyToNull(false)];
        yield 'inferSchema' => [static fn(GoogleSheetExtractor $e): mixed => $e->inferSchema(infer_schema())];
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [
            SheetValuesMother::batch([[['header'], ['row1']]]),
            SheetValuesMother::batch([[['header'], ['row1']]]),
        ]);
        $userSchema = schema(str_schema('header'));

        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
            ->withRowsPerPage(2)
            ->withSchema($userSchema)
            ->withMetadataColumns(true);

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()));
        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()));

        static::assertNull($userSchema->findDefinition('_spread_sheet_id'));
        static::assertNull($userSchema->findDefinition('_sheet_name'));
        static::assertSame([], $values->getCalls);
    }

    public function test_its_fails_if_sheet_not_found(): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(
            GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
        )
            ->withRowsPerPage(2)
            ->withMetadataColumns(true);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max rows "0" must be greater than 0');

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()));
    }

    public function test_schema_fails_the_same_way_for_a_missing_sheet(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max rows "0" must be greater than 0');

        GoogleSheetFixtureContext::extractor(
            GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
        )->schema();
    }

    public function test_schema_of_everything_fails_on_a_missing_sheet(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('End row "0" must be greater than 0');

        GoogleSheetFixtureContext::extractor(
            GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
        )
            ->inferSchema(infer_schema()->everyRow())
            ->schema();
    }

    /**
     * @param list<string> $cells
     */
    #[TestWith([['4', '5', '6', '7', '8']])]
    #[TestWith([['4', '5']])]
    public function test_a_row_whose_cell_count_differs_from_the_header_is_refused(array $cells): void
    {
        try {
            ExtractedRows::of(GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [
                SheetValuesMother::batch([[['a', 'b', 'c'], ['1', '2', '3'], $cells]]),
            ])))->withSchema(schema(str_schema('a'), str_schema('b'), str_schema('c', true))));
            static::fail('Expected the corrupted record to be refused');
        } catch (CorruptedRecordException $e) {
            static::assertSame([3, null, $cells], [$e->offset, $e->lineNumber, $e->fields]);
        }
    }

    public function test_a_declared_column_the_header_lacks_is_a_schema_error(): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage('"x"');

        ExtractedRows::of(GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [
            SheetValuesMother::batch([[['a', 'b', 'c'], ['1', '2', '3']]]),
        ])))->withSchema(schema(str_schema('a'), str_schema('b'), str_schema('x', true))));
    }

    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row(): void
    {
        $rows = ExtractedRows::of(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [
                SheetValuesMother::batch([[['header'], ['row1']], [['row2']]]),
            ])))
                ->withRowsPerPage(2)
                ->withSchema(schema(str_schema('header')))
                ->withMetadataColumns(true),
        );

        static::assertCount(2, $rows);
        static::assertSame(
            [
                ['header' => 'row1', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
                ['header' => 'row2', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
            ],
            $rows->toArray(),
        );
    }

    public function test_rows_in_batch_must_be_positive_integer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows per page must be greater than 0');

        GoogleSheetFixtureContext::extractor(new Sheets())->withRowsPerPage(0);
    }

    public function test_schema_appends_the_metadata_columns(): void
    {
        static::assertEquals(
            schema(str_schema('header'), str_schema('_spread_sheet_id'), str_schema('_sheet_name')),
            GoogleSheetFixtureContext::extractor(new Sheets())
                ->withSchema(schema(str_schema('header')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('header')),
            GoogleSheetFixtureContext::extractor(new Sheets())
                ->withSchema(schema(str_schema('header')))
                ->schema(),
        );
    }

    public function test_schema_is_inferred_from_a_sample_when_it_was_not_declared(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'name'], ['1', 'a'], ['2', '']]));

        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->schema()
                ->isSame(schema(int_schema('id', true), str_schema('name', true))),
        );
        static::assertSame([['spread-id', 'sheet!A1:B100', []]], $values->getCalls);
    }

    public function test_schema_appends_the_metadata_columns_to_the_inferred_schema(): void
    {
        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                100,
                GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']])),
            ))
                ->withMetadataColumns(true)
                ->schema()
                ->isSame(schema(int_schema('id', true), str_schema('_spread_sheet_id'), str_schema('_sheet_name'))),
        );
    }

    public function test_a_sheet_the_sample_covered_is_not_fetched_twice(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([['id'], ['1'], ['2']])], []);
        $service = GoogleSheetFixtureContext::service(3, $values);

        self::assertExtractedRowsCount(2, GoogleSheetFixtureContext::extractor($service));

        // no batchGet at all: the read was served from the rows the sample already held
        static::assertCount(1, $values->getCalls);
        static::assertSame([], $values->batchGetCalls);

        /** @var StubSpreadsheetsResource $spreadsheets */
        $spreadsheets = $service->spreadsheets;
        static::assertSame(1, $spreadsheets->calls);
    }

    public function test_a_sheet_larger_than_the_sample_is_still_read_in_full(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id'],
            ['1'],
            ['2'],
        ])], [SheetValuesMother::batch([[['id'], ['1'], ['2'], ['3']]])]);

        self::assertExtractedRowsCount(
            3,
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values)),
        );
        static::assertCount(1, $values->batchGetCalls);
    }

    public function test_the_sample_is_taken_once(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id'],
            ['1'],
        ])], [SheetValuesMother::batch([[['id'], ['1']]])]);
        $service = GoogleSheetFixtureContext::service(500, $values);
        $extractor = GoogleSheetFixtureContext::extractor($service);

        $extractor->schema();
        $extractor->schema();
        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()));

        static::assertCount(1, $values->getCalls);

        /** @var StubSpreadsheetsResource $spreadsheets */
        $spreadsheets = $service->spreadsheets;
        static::assertSame(2, $spreadsheets->calls);
    }

    #[DataProvider('memo_resetting_setters')]
    public function test_shape_changing_setters_reset_the_memo(Closure $setter): void
    {
        $values = GoogleSheetFixtureContext::values(
            SheetValuesMother::range([['id'], ['1']]),
            SheetValuesMother::range([['id'], ['1']]),
        );
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values));

        $extractor->schema();
        $setter($extractor);
        $extractor->schema();

        static::assertCount(2, $values->getCalls);
    }

    #[DataProvider('memo_keeping_setters')]
    public function test_paging_and_metadata_setters_keep_the_memo(Closure $setter): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values));

        $extractor->schema();
        $setter($extractor);
        $extractor->schema();

        static::assertCount(1, $values->getCalls);
    }

    public function test_infer_schema_knobs_reach_the_sample(): void
    {
        $values = GoogleSheetFixtureContext::values(
            SheetValuesMother::range([['id'], ['1']]),
            SheetValuesMother::range([['id'], ['1']]),
        );
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values));

        $extractor->inferSchema(infer_schema()->sampleSize(2))->schema();
        $extractor->inferSchema(infer_schema()->everyRow())->schema();

        static::assertSame('sheet!A1:B3', $values->getCalls[0][1]);
        static::assertSame('sheet!A1:B100', $values->getCalls[1][1]);
    }

    public function test_all_strings_floors_every_column(): void
    {
        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                100,
                GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'price'], ['1', '1.5']])),
            ))
                ->inferSchema(infer_schema()->allStrings())
                ->schema()
                ->isSame(schema(str_schema('id', true), str_schema('price', true))),
        );
    }

    public function test_extract_seeds_every_batch_with_the_inferred_schema(): void
    {
        // a grid larger than the sample, so the read is a real second fetch and can hold a row the sample missed
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
            ['2', 'b'],
        ])], [SheetValuesMother::batch([[['id', 'name'], ['1', 'a'], ['2', 'b'], ['3', 'c']]])])))->withMetadataColumns(
            true,
        );

        $sizes = [];
        $first = null;

        foreach (FlowTestCase::extracted(
            $extractor->withBatchSize(2),
            flow_context()->backend(),
            new Criteria(),
        ) as $rows) {
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
            $sizes[] = $rows->count();
            $first ??= $rows->toArray()[0];
        }

        static::assertLessThanOrEqual(2, max($sizes));
        static::assertContains(2, $sizes);
        static::assertSame(3, array_sum($sizes));
        static::assertSame(
            ['id' => 1, 'name' => 'a', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
            $first,
        );
    }

    public function test_extract_refuses_a_header_that_diverges_from_the_sample(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
        ])], [SheetValuesMother::batch([[['id', 'title'], ['1', 'a']]])]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/spreadsheet "spread-id" sheet "sheet"/');
        $this->expectExceptionMessageMatches('/unexpected \[title\], missing \[name\]/');

        iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values)),
            flow_context()->backend(),
            new Criteria(),
        ));
    }

    public function test_union_by_name_skips_the_divergence_check(): void
    {
        static::assertCount(
            1,
            iterator_to_array(FlowTestCase::extracted(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
                    ['id', 'name'],
                    ['1', 'a'],
                ])], [SheetValuesMother::batch([[['id', 'title'], ['1', 'a']]])])))->inferSchema(
                    infer_schema()->unionByName(),
                ),
                flow_context()->backend(),
                new Criteria(),
            )),
        );
    }

    public function test_extract_refuses_a_batch_missing_a_sampled_column(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
        ])], [SheetValuesMother::batch([[['id'], ['1']]])]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/unexpected \[\], missing \[name\]/');

        iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values)),
            flow_context()->backend(),
            new Criteria(),
        ));
    }

    public function test_extract_refuses_a_batch_with_a_column_the_sample_never_saw(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
        ])], [SheetValuesMother::batch([[
            ['id', 'name', 'extra'],
            ['1',  'a',    'x'],
        ]])]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/unexpected \[extra\], missing \[\]/');

        iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values)),
            flow_context()->backend(),
            new Criteria(),
        ));
    }

    public function test_metadata_columns_keep_a_numeric_column_name(): void
    {
        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
                ['3', '7'],
                ['a', 'b'],
            ])], [SheetValuesMother::batch([[['3', '7'], ['a', 'b']]])])))->withMetadataColumns(true),
            flow_context()->backend(),
            new Criteria(),
        ));

        static::assertSame(
            ['3' => 'a', '7' => 'b', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
            $rowsArray[0]->toArray()[0],
        );
    }

    public function test_extract_accepts_the_sampled_columns_in_another_order(): void
    {
        static::assertCount(
            1,
            iterator_to_array(FlowTestCase::extracted(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
                    ['id', 'name'],
                    ['1', 'a'],
                ])], [SheetValuesMother::batch([[['name', 'id'], ['a', '1']]])]))),
                flow_context()->backend(),
                new Criteria(),
            )),
        );
    }

    public function test_a_declared_schema_skips_the_sample_and_the_check(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
            ['id', 'extra'],
            ['1', 'x'],
        ]])]);

        static::assertCount(
            1,
            iterator_to_array(FlowTestCase::extracted(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                    100,
                    $values,
                ))->withSchema(schema(str_schema('id'))),
                flow_context()->backend(),
                new Criteria(),
            )),
        );
        static::assertSame([], $values->getCalls);
    }

    public function test_empty_cells_are_null_by_default(): void
    {
        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
                ['id', 'name'],
                ['1', ''],
            ]])])))->withSchema(schema(str_schema('id'), str_schema('name', nullable: true))),
            flow_context()->backend(),
            new Criteria(),
        ));

        static::assertNull($rowsArray[0]->toArray()[0]['name']);
    }

    public function test_empty_cells_stay_strings_when_asked(): void
    {
        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
                ['id', 'name'],
                ['1', ''],
            ]])])))
                ->withEmptyToNull(false)
                ->withSchema(schema(str_schema('id'), str_schema('name', nullable: true))),
            flow_context()->backend(),
            new Criteria(),
        ));

        static::assertSame('', $rowsArray[0]->toArray()[0]['name']);
    }

    public function test_a_value_past_the_sample_that_does_not_fit_is_refused(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id'],
            ['1'],
        ])], [SheetValuesMother::batch([[['id'], ['1'], ['n/a']]])]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/column "id"/');
        $this->expectExceptionMessageMatches('/offset 3 of google-sheet:\/\/spread-id\/sheet/');

        iterator_to_array(FlowTestCase::extracted(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                500,
                $values,
            ))->inferSchema(infer_schema()->sampleSize(1)),
            flow_context()->backend(),
            new Criteria(),
        ));
    }

    public function test_a_leading_blank_row_without_a_header_is_not_a_divergence(): void
    {
        $rows = [[], ['1', 'a'], ['2', 'b']];

        // a leading blank row is skipped before the generated header is sized, so the two data rows keep their columns
        self::assertExtractedRowsCount(
            2,
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range(
                $rows,
            )], [SheetValuesMother::batch([$rows])])))->withHeader(false),
        );
    }

    public function test_works_for_no_data(): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range(
            null,
        )], [SheetValuesMother::batch([null])])))->withRowsPerPage(20);

        static::assertCount(
            0,
            iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria())),
        );

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_google_sheet() has nothing to infer a schema from: spreadsheet "spread-id" sheet "sheet" carries no '
            . 'column. Declare the schema with ->withSchema(...).',
        );

        $extractor->schema();
    }

    public function test_extract_without_a_header_row_infers_generated_names(): void
    {
        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                100,
                GoogleSheetFixtureContext::values(SheetValuesMother::range([['1', 'a']])),
            ))
                ->withHeader(false)
                ->schema()
                ->isSame(schema(int_schema('e00', true), str_schema('e01', true))),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(GoogleSheetFixtureContext::extractor(new Sheets())->isRepeatable());
    }

    public function test_it_declares_the_grid_row_count_as_an_upper_bound(): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']])),
        ));
        $extractor->schema();

        static::assertEquals(Cardinality::atMost(100), $extractor->statistics()->rows);
    }

    public function test_it_declares_no_byte_size(): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']])),
        ));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->size);
    }

    public function test_the_grid_row_count_costs_no_extra_request(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));
        $service = GoogleSheetFixtureContext::service(100, $values);
        $extractor = GoogleSheetFixtureContext::extractor($service);

        $extractor->schema();

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertCount(1, $values->getCalls);

        /** @var StubSpreadsheetsResource $spreadsheets */
        $spreadsheets = $service->spreadsheets;
        static::assertSame(1, $spreadsheets->calls);
    }

    public function test_statistics_without_a_sample_declare_nothing_and_request_nothing(): void
    {
        $values = GoogleSheetFixtureContext::values();
        $service = GoogleSheetFixtureContext::service(100, $values);

        static::assertEquals(new Statistics(), GoogleSheetFixtureContext::extractor($service)->statistics());
        static::assertSame([], $values->getCalls);

        /** @var StubSpreadsheetsResource $spreadsheets */
        $spreadsheets = $service->spreadsheets;
        static::assertSame(0, $spreadsheets->calls);
    }

    public function test_a_read_samples_the_grid_row_count_too(): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(3, GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id'],
            ['1'],
            ['2'],
        ])], [])));

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false);

        static::assertEquals(Cardinality::atMost(3), $extractor->statistics()->rows);
    }

    #[DataProvider('memo_resetting_setters')]
    public function test_shape_changing_setters_drop_the_sampled_statistics(Closure $setter): void
    {
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']])),
        ));

        $extractor->schema();
        $extractor->statistics();
        $setter($extractor);

        static::assertEquals(new Statistics(), $extractor->statistics());
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $backend = new SpyBackend();

        iterator_to_array(
            FlowTestCase::extracted(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
                    ['header'],
                    ['row1'],
                ]])])))->withSchema(schema(str_schema('header'))),
                flow_context(config_builder()->backend($backend)->build())->backend(),
                new Criteria(),
            ),
            false,
        );

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    public function test_corrupted_records_are_routed_to_the_reject_loader(): void
    {
        $rejects = [];

        $rows = df()
            ->read(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::valuesAndBatches([], [
                    SheetValuesMother::batch([[['a', 'b'], ['1', '2'], ['3', '4', '5'], ['6', '7']]]),
                ])))
                    ->withSchema(schema(str_schema('a'), str_schema('b')))
                    ->withCorruptedRecords(to_array($rejects)),
            )
            ->fetch();

        static::assertSame(['1', '6'], $rows->reduceToArray('a'));
        static::assertSame(
            [[3, '3 fields, the header has 2', ['3', '4', '5']]],
            array_map(static fn(array $reject): array => [
                $reject['offset'],
                $reject['reason'],
                $reject['fields'],
            ], $rejects),
        );
    }
}
