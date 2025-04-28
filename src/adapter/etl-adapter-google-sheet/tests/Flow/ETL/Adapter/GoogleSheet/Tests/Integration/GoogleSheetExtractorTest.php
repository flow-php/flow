<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\GoogleSheet\{
    Tests\GoogleSheetsContext,
    Tests\HttpClientContext,
    Tests\HttpRequestContext};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    private GoogleSheetsContext $context;

    protected function setUp() : void
    {
        $this->context = new GoogleSheetsContext(
            (new HttpClientContext())
                ->add(
                    new HttpRequestContext(
                        'GET',
                        'https://sheets.googleapis.com/v4/spreadsheets/1234567890/values/%27Sheet%27%21A1%3AZ1000',
                    ),
                    __DIR__ . '/../Fixtures/extra-columns.json'
                )
        );
    }

    public function test_extract_with_cut_extra_columns() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(),
            '1234567890',
            'Sheet',
        );

        $rows = $extractor->extract(flow_context(config()));

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }

    public function test_extract_without_cut_extra_columns() : void
    {
        $extractor = from_google_sheet(
            $this->context->sheets(),
            '1234567890',
            'Sheet',
        );
        $extractor->withDropExtraColumns(false);

        $rows = $extractor->extract(flow_context(config()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }
}
