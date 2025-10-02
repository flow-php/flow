<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\{df, int_schema, schema, string_schema};
use Flow\ETL\Adapter\GoogleSheet\{Tests\GoogleSheetsContext};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    private GoogleSheetsContext $context;

    protected function setUp() : void
    {
        $this->context = new GoogleSheetsContext();
    }

    public function test_extract_puts_null_in_not_matching_schema_rows() : void
    {
        $rows = df()
            ->extract(
                from_google_sheet(
                    $this->context->sheets(__DIR__ . '/../Fixtures/extra-empty-rows.json'),
                    '1234567890',
                    'Sheet',
                )->withSchema(
                    schema(
                        string_schema('Header 1'),
                        string_schema('Header 2'),
                        int_schema('id'),
                    )
                )
            )
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            self::assertNotSame([], $row);
            self::assertArrayNotHasKey('Header 3', $row);
            self::assertNull($row['id']);
        }
    }

    public function test_extract_skip_extra_empty_rows() : void
    {
        $rows = df()
            ->extract(
                from_google_sheet(
                    $this->context->sheets(__DIR__ . '/../Fixtures/extra-empty-rows.json'),
                    '1234567890',
                    'Sheet',
                )
            )
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            self::assertNotSame([], $row);
        }
    }

    public function test_extract_with_cut_extra_columns() : void
    {
        $rows = df()
            ->extract(
                from_google_sheet(
                    $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
                    '1234567890',
                    'Sheet',
                )
            )
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }

    public function test_extract_without_cut_extra_columns() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        df()
            ->extract(
                from_google_sheet(
                    $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
                    '1234567890',
                    'Sheet',
                )->withDropExtraColumns(false)
            )
            ->fetch()
            ->toArray();
    }
}
