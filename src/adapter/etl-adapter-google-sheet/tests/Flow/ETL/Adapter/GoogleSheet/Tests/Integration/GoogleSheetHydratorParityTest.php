<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;

final class GoogleSheetHydratorParityTest extends FlowTestCase
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
            foreach ($batch as $row) {
                $rows[] = $row->toArray();
            }
        }

        static::assertNotEmpty($rows);

        foreach ($rows as $row) {
            static::assertSame('1234567890', $row['_spread_sheet_id']);
            static::assertSame('Sheet', $row['_sheet_name']);
        }
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_google_sheet(
            (new GoogleSheetsContext())->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );

        $count = 0;

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $batch) {
            $count += $batch->count();
        }

        static::assertSame(19, $count);
    }
}
