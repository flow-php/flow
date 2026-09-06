<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetReadOptions;
use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\StubSpreadsheetsResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class GoogleSheetReaderTest extends FlowTestCase
{
    public function test_row_count_is_read_from_the_sheet_grid(): void
    {
        $service = GoogleSheetFixtureContext::service(100, GoogleSheetFixtureContext::values());

        static::assertSame(100, GoogleSheetFixtureContext::reader($service)->rowCount());

        /** @var StubSpreadsheetsResource $spreadsheets */
        $spreadsheets = $service->spreadsheets;
        static::assertSame(1, $spreadsheets->calls);
    }

    public function test_row_count_is_zero_when_the_sheet_is_absent(): void
    {
        static::assertSame(
            0,
            GoogleSheetFixtureContext::reader(
                GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
            )->rowCount(),
        );
    }

    public function test_row_count_ignores_a_sheet_with_another_name(): void
    {
        static::assertSame(
            0,
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(
                100,
                GoogleSheetFixtureContext::values(),
                'another',
            ))->rowCount(),
        );
    }

    public function test_sample_requests_the_header_plus_the_requested_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'name'], ['1', 'a'], ['2', 'b']]));

        GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values))->sample(2);

        static::assertSame([['spread-id', 'sheet!A1:B3', []]], $values->getCalls);
    }

    public function test_sample_without_a_header_requests_exactly_the_requested_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['1', 'a'], ['2', 'b']]));

        $sample = GoogleSheetFixtureContext::reader(
            GoogleSheetFixtureContext::service(100, $values),
            new GoogleSheetReadOptions(withHeader: false),
        )->sample(2);

        static::assertSame([['spread-id', 'sheet!A1:B2', []]], $values->getCalls);
        static::assertSame(['e00', 'e01'], $sample->names);
    }

    public function test_sample_is_clamped_to_the_grid(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'name']]));

        GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(5, $values))->sample(20_480);

        static::assertSame('sheet!A1:B5', $values->getCalls[0][1]);
    }

    public function test_sample_of_everything_requests_the_whole_grid(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'name']]));

        GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values))->sample(-1);

        static::assertSame('sheet!A1:B100', $values->getCalls[0][1]);
    }

    public function test_sample_forwards_the_render_options(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id']]));
        $options = ['valueRenderOption' => 'UNFORMATTED_VALUE', 'dateTimeRenderOption' => 'FORMATTED_STRING'];

        GoogleSheetFixtureContext::reader(
            GoogleSheetFixtureContext::service(100, $values),
            new GoogleSheetReadOptions(options: $options),
        )->sample(2);

        static::assertSame($options, $values->getCalls[0][2]);
    }

    public function test_sample_decodes_names_and_rows_through_its_own_encoder(): void
    {
        $sample = GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note'], ['1', ''], ['2', 'x']])),
        ))->sample(2);

        static::assertSame(['id', 'note'], $sample->names);
        static::assertSame(['id' => '1', 'note' => null], $sample->rows[0]->values);
        static::assertCount(2, $sample->rows);
    }

    public function test_sample_of_a_header_only_sheet_has_names_and_no_rows(): void
    {
        $sample = GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note']])),
        ))->sample(2);

        static::assertSame(['id', 'note'], $sample->names);
        static::assertSame([], $sample->rows);
    }

    public function test_sample_of_an_empty_range_has_neither(): void
    {
        $sample = GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(
            100,
            GoogleSheetFixtureContext::values(SheetValuesMother::range(null)),
        ))->sample(2);

        static::assertSame([], $sample->names);
        static::assertSame([], $sample->rows);
    }

    public function test_sample_of_an_absent_sheet_fails(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max rows "0" must be greater than 0');

        GoogleSheetFixtureContext::reader(
            GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
        )->sample(20_480);
    }

    public function test_sample_of_everything_on_an_absent_sheet_fails(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('End row "0" must be greater than 0');

        GoogleSheetFixtureContext::reader(
            GoogleSheetFixtureContext::serviceWithoutSheets(GoogleSheetFixtureContext::values()),
        )->sample(-1);
    }

    public function test_batches_page_the_sheet_by_rows_per_page(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([
            [['id'], ['1']],
            [['2'], ['3']],
            [['4']],
        ])]);

        iterator_to_array(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(5, $values))->batches(2, 100),
            false,
        );

        static::assertSame(['sheet!A1:B2', 'sheet!A3:B4', 'sheet!A5:B5'], $values->batchGetCalls[0][1]['ranges']);
    }

    public function test_batches_forward_the_render_options(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[['id'], ['1']]])]);

        iterator_to_array(
            GoogleSheetFixtureContext::reader(
                GoogleSheetFixtureContext::service(2, $values),
                new GoogleSheetReadOptions(options: ['valueRenderOption' => 'UNFORMATTED_VALUE']),
            )->batches(2, 100),
            false,
        );

        static::assertSame('UNFORMATTED_VALUE', $values->batchGetCalls[0][1]['valueRenderOption']);
        static::assertSame(['sheet!A1:B2'], $values->batchGetCalls[0][1]['ranges']);
    }

    public function test_batches_are_cut_at_the_batch_size_across_pages(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([
            [['id'], ['1'], ['2'], ['3']],
            [['4'], ['5'], ['6']],
        ])]);

        $batches = iterator_to_array(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(10, $values))->batches(10, 4),
            false,
        );

        static::assertSame([4, 2], array_map('count', $batches));
    }

    public function test_batches_skip_pages_without_values(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([[['id'], ['1']], null])]);

        $batches = iterator_to_array(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(4, $values))->batches(2, 100),
            false,
        );

        static::assertSame([1], array_map('count', $batches));
    }

    public function test_batches_yield_nothing_for_an_empty_sheet(): void
    {
        $values = GoogleSheetFixtureContext::valuesAndBatches([], [SheetValuesMother::batch([null])]);

        static::assertSame(
            [],
            iterator_to_array(
                GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(2, $values))->batches(2, 100),
                false,
            ),
        );
    }

    public function test_infer_schema_types_formatted_strings_through_the_ladder(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([
            ['id', 'price', 'ok',    'day',        'note'],
            ['1',  '1.5',   'TRUE',  '2024-01-01', ''],
            ['2',  '',      'FALSE', '2024-01-02', 'x'],
        ]));

        static::assertTrue(GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values)),
            infer_schema()->build(),
        )->isSame(schema(
            int_schema('id', true),
            float_schema('price', true),
            bool_schema('ok', true),
            date_schema('day', true),
            str_schema('note', true),
        )));
    }

    public function test_infer_schema_reads_an_empty_cell_as_string_evidence_when_empty_to_null_is_off(): void
    {
        $options = new GoogleSheetReadOptions(emptyToNull: false);
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([
            ['id', 'price'],
            ['1', '1.5'],
            ['2', ''],
        ]));

        static::assertTrue(GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values), $options),
            infer_schema()->build(),
            $options,
        )->isSame(schema(int_schema('id', true), str_schema('price', true))));
    }

    public function test_infer_schema_types_unformatted_values_as_the_api_typed_them(): void
    {
        $options = new GoogleSheetReadOptions(options: ['valueRenderOption' => 'UNFORMATTED_VALUE']);
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([
            ['id', 'price', 'ok', 'day'],
            [1,    1.5,     true, '2024-01-01'],
        ]));

        static::assertTrue(GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values), $options),
            infer_schema()->build(),
            $options,
        )->isSame(schema(
            int_schema('id', true),
            float_schema('price', true),
            bool_schema('ok', true),
            str_schema('day', true),
        )));
    }

    public function test_infer_schema_of_a_header_only_sheet_is_every_header_as_nullable_string(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'price']]));

        static::assertTrue(GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values)),
            infer_schema()->build(),
        )->isSame(schema(str_schema('id', true), str_schema('price', true))));
    }

    public function test_infer_schema_of_an_empty_sheet_is_empty(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range(null));

        static::assertCount(
            0,
            GoogleSheetFixtureContext::inferFrom(
                GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values)),
                infer_schema()->build(),
            )->definitions(),
        );
    }

    public function test_infer_schema_honours_all_strings(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'price'], ['1', '1.5']]));

        static::assertTrue(GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values)),
            infer_schema()->allStrings()->build(),
        )->isSame(schema(str_schema('id', true), str_schema('price', true))));
    }

    public function test_infer_schema_asks_for_sample_size_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));

        GoogleSheetFixtureContext::inferFrom(
            GoogleSheetFixtureContext::reader(GoogleSheetFixtureContext::service(100, $values)),
            infer_schema()->sampleSize(5)->build(),
        );

        static::assertSame('sheet!A1:B6', $values->getCalls[0][1]);
    }
}
