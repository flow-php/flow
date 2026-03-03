<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\{df, int_schema, schema, string_schema};
use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    private GoogleSheetsContext $context;

    protected function setUp() : void
    {
        $this->context = new GoogleSheetsContext();
    }

    public function test_extract_expand_missing_columns() : void
    {
        $rows = df()
            ->extract(
                from_google_sheet(
                    $this->context->sheets(__DIR__ . '/../Fixtures/missing-columns.json'),
                    '1234567890',
                    'Sheet',
                )
            )
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            self::assertCount(3, $row);
        }
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

        self::assertCount(10, $rows);

        foreach ($rows as $row) {
            self::assertNotSame([], $row);
        }
    }

    public function test_extract_with_batches() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(19, $rows);
    }

    public function test_extract_with_batches_containing_empty_rows() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/batch-empty-rows.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(9, $rows);
    }

    public function test_extract_with_batches_without_header() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);
        $extractor->withHeader(false);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(20, $rows);
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

    public function test_extract_with_limit() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->changeLimit(2);

        $rows = df()
            ->extract($extractor)
            ->fetch()
            ->toArray();

        self::assertCount(2, $rows);
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
