<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Closure;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\StubSpreadsheetsResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Google\Service\Sheets;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

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
        yield 'withDropExtraColumns' => [static fn(GoogleSheetExtractor $e): mixed => $e->withDropExtraColumns(false)];
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

        iterator_to_array($extractor->extract(flow_context()));
        iterator_to_array($extractor->extract(flow_context()));

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

        iterator_to_array($extractor->extract(flow_context()));
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
            ->inferSchema(infer_schema()->sampleSize(-1))
            ->schema();
    }

    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [
            SheetValuesMother::batch([[['header'], ['row1']], [['row2']]]),
        ]);

        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
            ->withRowsPerPage(2)
            ->withSchema(schema(str_schema('header')))
            ->withMetadataColumns(true);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array($extractor->extract(flow_context()));

        static::assertCount(2, $rowsArray);
        static::assertEquals(
            row(['_sheet_name' => 'sheet', '_spread_sheet_id' => 'spread-id', 'header' => 'row1']),
            $rowsArray[0]->first(),
        );
        static::assertEquals(
            row(['_sheet_name' => 'sheet', '_spread_sheet_id' => 'spread-id', 'header' => 'row2']),
            $rowsArray[1]->first(),
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
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));

        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->withMetadataColumns(true)
                ->schema()
                ->isSame(schema(int_schema('id', true), str_schema('_spread_sheet_id'), str_schema('_sheet_name'))),
        );
    }

    public function test_a_sheet_the_sample_covered_is_not_fetched_twice(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([['id'], ['1'], ['2']])], []);
        $service = GoogleSheetFixtureContext::service(3, $values);

        static::assertCount(
            2,
            iterator_to_array(GoogleSheetFixtureContext::extractor($service)->extract(flow_context())),
        );

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

        static::assertCount(
            3,
            iterator_to_array(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values))->extract(
                    flow_context(),
                ),
            ),
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
        iterator_to_array($extractor->extract(flow_context()));

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
        $extractor->inferSchema(infer_schema()->sampleSize(-1))->schema();

        static::assertSame('sheet!A1:B3', $values->getCalls[0][1]);
        static::assertSame('sheet!A1:B100', $values->getCalls[1][1]);
    }

    public function test_all_strings_floors_every_column(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'price'], ['1', '1.5']]));

        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->inferSchema(infer_schema()->allStrings())
                ->schema()
                ->isSame(schema(str_schema('id', true), str_schema('price', true))),
        );
    }

    public function test_extract_seeds_every_batch_with_the_inferred_schema(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
            ['2', 'b'],
        ])], [SheetValuesMother::batch([[['id', 'name'], ['1', 'a'], ['2', 'b'], ['3', 'c']]])]);
        // a grid larger than the sample, so the read is a real second fetch and can hold a row the sample missed
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
            500,
            $values,
        ))->withMetadataColumns(true);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array($extractor->extract(
            flow_context(config_builder()->extractorBatchSize(2)->build()),
        ));

        static::assertCount(3, $rowsArray);

        foreach ($rowsArray as $rows) {
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
        }

        static::assertSame(
            ['id' => 1, 'name' => 'a', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
            $rowsArray[0]->first()->toArray(),
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

        iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values))->extract(
                flow_context(),
            ),
        );
    }

    public function test_union_by_name_skips_the_divergence_check(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
        ])], [SheetValuesMother::batch([[['id', 'title'], ['1', 'a']]])]);

        static::assertCount(
            1,
            iterator_to_array(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                    ->inferSchema(infer_schema()->unionByName())
                    ->extract(flow_context()),
            ),
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

        iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values))->extract(
                flow_context(),
            ),
        );
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

        iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values))->extract(
                flow_context(),
            ),
        );
    }

    public function test_metadata_columns_keep_a_numeric_column_name(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['3', '7'],
            ['a', 'b'],
        ])], [SheetValuesMother::batch([[['3', '7'], ['a', 'b']]])]);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->withMetadataColumns(true)
                ->extract(flow_context()),
        );

        static::assertSame(
            ['3' => 'a', '7' => 'b', '_spread_sheet_id' => 'spread-id', '_sheet_name' => 'sheet'],
            $rowsArray[0]->first()->toArray(),
        );
    }

    public function test_extract_accepts_the_sampled_columns_in_another_order(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id', 'name'],
            ['1', 'a'],
        ])], [SheetValuesMother::batch([[['name', 'id'], ['a', '1']]])]);

        static::assertCount(
            1,
            iterator_to_array(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))->extract(
                    flow_context(),
                ),
            ),
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
            iterator_to_array(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                    ->withSchema(schema(str_schema('id')))
                    ->extract(flow_context()),
            ),
        );
        static::assertSame([], $values->getCalls);
    }

    public function test_empty_cells_are_null_by_default(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
            ['id', 'name'],
            ['1', ''],
        ]])]);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->withSchema(schema(str_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
        );

        static::assertNull($rowsArray[0]->first()->toArray()['name']);
    }

    public function test_empty_cells_stay_strings_when_asked(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[
            ['id', 'name'],
            ['1', ''],
        ]])]);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->withEmptyToNull(false)
                ->withSchema(schema(str_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
        );

        static::assertSame('', $rowsArray[0]->first()->toArray()['name']);
    }

    public function test_a_value_past_the_sample_that_does_not_fit_is_refused(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
            ['id'],
            ['1'],
        ])], [SheetValuesMother::batch([[['id'], ['1'], ['n/a']]])]);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessageMatches('/column "id"/');
        $this->expectExceptionMessageMatches('/row 1/');

        iterator_to_array(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(500, $values))
                ->inferSchema(infer_schema()->sampleSize(1))
                ->extract(flow_context()),
        );
    }

    public function test_a_leading_blank_row_without_a_header_is_not_a_divergence(): void
    {
        $rows = [[], ['1', 'a'], ['2', 'b']];
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range(
            $rows,
        )], [SheetValuesMother::batch([$rows])]);

        // 3, not 2: the blank row still decodes to a column-less row, as it did before inference existed. What this
        // pins is that the divergence check no longer reads that row as "every column is missing".
        static::assertCount(
            3,
            iterator_to_array(
                GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                    ->withHeader(false)
                    ->extract(flow_context()),
            ),
        );
    }

    public function test_works_for_no_data(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range(
            null,
        )], [SheetValuesMother::batch([null])]);
        $extractor = GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
            100,
            $values,
        ))->withRowsPerPage(20);

        static::assertCount(0, $extractor->schema()->definitions());
        static::assertCount(0, iterator_to_array($extractor->extract(flow_context())));
    }

    public function test_extract_without_a_header_row_infers_generated_names(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['1', 'a']]));

        static::assertTrue(
            GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(100, $values))
                ->withHeader(false)
                ->schema()
                ->isSame(schema(int_schema('e00', true), str_schema('e01', true))),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(GoogleSheetFixtureContext::extractor(new Sheets())->isRepeatable());
    }
}
