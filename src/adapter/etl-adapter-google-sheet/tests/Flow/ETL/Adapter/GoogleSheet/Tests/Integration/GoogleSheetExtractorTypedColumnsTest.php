<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Config;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function reset;

final class GoogleSheetExtractorTypedColumnsTest extends FlowTestCase
{
    public function test_appends_spread_sheet_id_and_sheet_name_to_the_schema_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_google_sheet(
            (new GoogleSheetsContext())->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        )
            ->withSchema(schema(string_schema('Header 1'), string_schema('Header 2'), string_schema('Header 3')))
            ->withMetadataColumns(true);

        $rows = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            foreach ($batch->toArray() as $row) {
                $rows[] = $row;
            }
        }

        static::assertNotEmpty($rows);

        foreach ($rows as $row) {
            static::assertSame('1234567890', $row['_spread_sheet_id']);
            static::assertSame('Sheet', $row['_sheet_name']);
        }
    }

    public function test_infers_typed_rows(): void
    {
        $extractor = from_google_sheet(
            (new GoogleSheetsContext())->sheetsOfGrid(
                __DIR__ . '/../Fixtures/spreadsheet-large-grid.json',
                __DIR__ . '/../Fixtures/sample-typed.json',
                __DIR__ . '/../Fixtures/batch-typed.json',
            ),
            '1234567890',
            'Sheet',
        );

        $rows = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            static::assertTrue(
                $batch
                    ->schema()
                    ->isSame(schema(
                        int_schema('id', true),
                        float_schema('price', true),
                        bool_schema('active', true),
                        date_schema('day', true),
                        str_schema('note', true),
                    )),
            );

            foreach ($batch->toArray() as $row) {
                $rows[] = $row;
            }
        }

        static::assertCount(3, $rows);

        $first = reset($rows);

        static::assertIsArray($first);
        static::assertSame(1, $first['id']);
        static::assertSame(1.5, $first['price']);
        static::assertTrue($first['active']);
        static::assertNull($first['note']);
    }
}
