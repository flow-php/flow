<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use Flow\ETL\Adapter\GoogleSheet\Tests\GoogleSheetsContext;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;

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
                $this->context->sheets(__DIR__ . '/../Fixtures/missing-columns.json'),
                '1234567890',
                'Sheet',
            ))
            ->fetch()
            ->toArray();

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
                $this->context->sheets(__DIR__ . '/../Fixtures/extra-empty-rows.json'),
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
            $this->context->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(19, $rows);
    }

    public function test_extract_with_batches_containing_empty_rows(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/batch-empty-rows.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(9, $rows);
    }

    public function test_extract_with_batches_without_header(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/batch.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->withRowsPerPage(10);
        $extractor->withHeader(false);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(20, $rows);
    }

    public function test_extract_with_cut_extra_columns(): void
    {
        $rows = df()
            ->extract(from_google_sheet(
                $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
                '1234567890',
                'Sheet',
            ))
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            static::assertNotNull($row);
        }
    }

    public function test_extract_with_limit(): void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
            '1234567890',
            'Sheet',
        );
        $extractor->changeLimit(2);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(2, $rows);
    }

    public function test_extract_without_cut_extra_columns(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        df()
            ->extract(from_google_sheet(
                $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
                '1234567890',
                'Sheet',
            )->withDropExtraColumns(false))
            ->fetch()
            ->toArray();
    }
}
