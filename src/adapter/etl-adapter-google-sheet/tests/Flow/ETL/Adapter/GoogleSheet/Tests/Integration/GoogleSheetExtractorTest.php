<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Config;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function iterator_to_array;
use function str_contains;
use function urldecode;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    private GoogleSheetsContext $context;

    protected function setUp(): void
    {
        $this->context = new GoogleSheetsContext();
    }

    public function test_extract_expand_missing_columns(): void
    {
        $rows = df()
            ->extract(from_google_sheet(
                $this->context->sheets(
                    __DIR__ . '/../Fixtures/sample-missing-columns.json',
                    __DIR__ . '/../Fixtures/missing-columns.json',
                ),
                '1234567890',
                'Sheet',
            ))
            ->fetch()
            ->toArray();

        static::assertCount(4, $rows);

        foreach ($rows as $row) {
            static::assertCount(3, $row);
        }
    }

    public function test_extract_puts_null_in_a_declared_nullable_column_the_sheet_lacks(): void
    {
        $rows = df()
            ->extract(from_google_sheet(
                $this->context->sheets(__DIR__ . '/../Fixtures/extra-empty-rows.json'),
                '1234567890',
                'Sheet',
            )->withSchema(schema(
                string_schema('Header 1'),
                string_schema('Header 2'),
                int_schema('id', nullable: true),
            )))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);

        foreach ($rows as $row) {
            static::assertNotSame([], $row);
            static::assertArrayNotHasKey('Header 3', $row);
            static::assertNull($row['id']);
        }
    }

    /**
     * b57: the sheet has no such column, so every row would carry a null under a NOT NULL
     * declaration. Declare the column nullable if that is what the data is.
     */
    public function test_extract_refuses_a_not_null_column_the_sheet_lacks(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert null to integer, column is not nullable');

        df()
            ->extract(from_google_sheet(
                $this->context->sheets(__DIR__ . '/../Fixtures/extra-empty-rows.json'),
                '1234567890',
                'Sheet',
            )->withSchema(schema(string_schema('Header 1'), string_schema('Header 2'), int_schema('id'))))
            ->fetch();
    }

    public function test_extract_skip_extra_empty_rows(): void
    {
        $rows = df()
            ->extract(from_google_sheet(
                $this->context->sheets(
                    __DIR__ . '/../Fixtures/sample-extra-empty-rows.json',
                    __DIR__ . '/../Fixtures/extra-empty-rows.json',
                ),
                '1234567890',
                'Sheet',
            ))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);

        foreach ($rows as $row) {
            static::assertNotSame([], $row);
        }
    }

    public function test_extract_with_batches(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/sample-batch.json', __DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        static::assertCount(19, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_extract_with_batches_containing_empty_rows(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(
                __DIR__ . '/../Fixtures/sample-batch-empty-rows.json',
                __DIR__ . '/../Fixtures/batch-empty-rows.json',
            ),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        static::assertCount(9, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_extract_with_batches_without_header(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/sample-batch.json', __DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);
        $extractor->withHeader(false);

        static::assertCount(20, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_extract_with_cut_extra_columns(): void
    {
        $rows = df()
            ->extract(from_google_sheet(
                $this->context->sheets(
                    __DIR__ . '/../Fixtures/sample-extra-columns.json',
                    __DIR__ . '/../Fixtures/extra-columns.json',
                ),
                '1234567890',
                'Sheet',
            ))
            ->fetch()
            ->toArray();

        static::assertCount(10, $rows);

        foreach ($rows as $row) {
            static::assertCount(3, $row);
        }
    }

    public function test_extract_with_limit(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(
                __DIR__ . '/../Fixtures/sample-extra-columns.json',
                __DIR__ . '/../Fixtures/extra-columns.json',
            ),
            '1234567890',
            'Sheet',
        );
        $extractor->changeLimit(2);

        static::assertCount(2, df()->extract($extractor)->fetch()->toArray());
    }

    public function test_extract_without_cut_extra_columns(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        df()
            ->extract(from_google_sheet(
                $this->context->sheets(
                    __DIR__ . '/../Fixtures/sample-extra-columns.json',
                    __DIR__ . '/../Fixtures/extra-columns.json',
                ),
                '1234567890',
                'Sheet',
            )->withDropExtraColumns(false))
            ->fetch()
            ->toArray();
    }

    public function test_schema_is_inferred_before_any_row_is_read(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/sample-typed.json'),
            '1234567890',
            'Sheet',
        );

        static::assertTrue(
            $extractor
                ->schema()
                ->isSame(schema(
                    int_schema('id', true),
                    float_schema('price', true),
                    bool_schema('active', true),
                    date_schema('day', true),
                    str_schema('note', true),
                )),
        );

        $requests = $this->context->requests();
        static::assertCount(2, $requests);
        static::assertTrue(str_contains(urldecode((string) $requests[1]->getUri()), 'Sheet!A1:Z20'));
    }

    public function test_every_batch_carries_the_inferred_schema(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheetsOfGrid(
                __DIR__ . '/../Fixtures/spreadsheet-large-grid.json',
                __DIR__ . '/../Fixtures/sample-typed.json',
                __DIR__ . '/../Fixtures/batch-typed.json',
            ),
            '1234567890',
            'Sheet',
        );

        /** @var array<Rows> $batches */
        $batches = iterator_to_array($extractor->extract(
            flow_context(Config::builder()->extractorBatchSize(1)->build()),
        ));

        static::assertCount(3, $batches);

        foreach ($batches as $rows) {
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
        }

        $first = $batches[0]->first()->toArray();
        static::assertSame(1, $first['id']);
        static::assertSame(1.5, $first['price']);
        static::assertTrue($first['active']);
        static::assertNull($first['note']);
    }

    public function test_a_header_only_sheet_infers_nullable_strings_and_yields_nothing(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(
                __DIR__ . '/../Fixtures/sample-header-only.json',
                __DIR__ . '/../Fixtures/batch-header-only.json',
            ),
            '1234567890',
            'Sheet',
        );

        static::assertTrue($extractor->schema()->isSame(schema(str_schema('id', true), str_schema('price', true))));
        static::assertCount(0, df()->extract($extractor)->fetch());
    }

    public function test_an_empty_sheet_infers_an_empty_schema(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(
                __DIR__ . '/../Fixtures/sample-empty.json',
                __DIR__ . '/../Fixtures/batch-empty.json',
            ),
            '1234567890',
            'Sheet',
        );

        static::assertCount(0, $extractor->schema()->definitions());
        static::assertCount(0, df()->extract($extractor)->fetch());
    }

    public function test_unformatted_values_keep_the_types_the_api_gave_them(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(
                __DIR__ . '/../Fixtures/sample-unformatted.json',
                __DIR__ . '/../Fixtures/batch-unformatted.json',
            ),
            '1234567890',
            'Sheet',
        )->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE']);

        static::assertTrue(
            $extractor
                ->schema()
                ->isSame(schema(
                    int_schema('id', true),
                    float_schema('price', true),
                    bool_schema('active', true),
                    float_schema('day', true),
                    str_schema('note', true),
                )),
        );

        df()->extract($extractor)->fetch();

        foreach ([1, 3] as $valuesRequest) {
            static::assertTrue(str_contains(
                (string) $this->context->requests()[$valuesRequest]->getUri(),
                'valueRenderOption=UNFORMATTED_VALUE',
            ));
        }
    }

    public function test_a_header_that_changed_since_the_sample_is_refused(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessageMatches('/unexpected \[comment\], missing \[note\]/');

        df()
            ->extract(from_google_sheet(
                $this->context->sheetsOfGrid(
                    __DIR__ . '/../Fixtures/spreadsheet-large-grid.json',
                    __DIR__ . '/../Fixtures/sample-typed.json',
                    __DIR__ . '/../Fixtures/batch-typed-diverging.json',
                ),
                '1234567890',
                'Sheet',
            ))
            ->fetch();
    }

    public function test_a_value_past_the_sample_that_does_not_fit_is_refused(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessageMatches('/column "price"/');
        $this->expectExceptionMessageMatches('/row 3/');

        df()
            ->extract(from_google_sheet(
                $this->context->sheetsOfGrid(
                    __DIR__ . '/../Fixtures/spreadsheet-large-grid.json',
                    __DIR__ . '/../Fixtures/sample-typed.json',
                    __DIR__ . '/../Fixtures/batch-typed-misfit.json',
                ),
                '1234567890',
                'Sheet',
            )->inferSchema(infer_schema()->sampleSize(2)))
            ->fetch();
    }

    public function test_limit_still_pays_the_sample(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheetsOfGrid(
                __DIR__ . '/../Fixtures/spreadsheet-large-grid.json',
                __DIR__ . '/../Fixtures/sample-typed.json',
                __DIR__ . '/../Fixtures/batch-typed.json',
            ),
            '1234567890',
            'Sheet',
        );
        $extractor->changeLimit(1);

        static::assertCount(1, df()->extract($extractor)->fetch()->toArray());
        static::assertCount(4, $this->context->requests());
    }
}
