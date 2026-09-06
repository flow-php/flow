<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

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

final class GoogleSheetHydratorParityTest extends FlowTestCase
{
    public static function hydrators(): iterable
    {
        yield 'php' => [new PhpRowHydrator()];
        yield 'adaptive' => [new AdaptiveRowHydrator()];
    }

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
            (new GoogleSheetsContext())->sheets(
                __DIR__ . '/../Fixtures/sample-batch.json',
                __DIR__ . '/../Fixtures/batch.json',
            ),
            '1234567890',
            'Sheet',
        );

        $count = 0;

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $batch) {
            static::assertTrue($batch->schema()->isSame($extractor->schema()));
            $count += $batch->count();
        }

        static::assertSame(19, $count);
    }

    #[DataProvider('hydrators')]
    public function test_infers_the_same_typed_rows_on_both_hydrators(Hydrator $hydrator): void
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

        foreach ($extractor->extract(flow_context(Config::builder()->hydrator($hydrator)->build())) as $batch) {
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

            foreach ($batch as $row) {
                $rows[] = $row->toArray();
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
